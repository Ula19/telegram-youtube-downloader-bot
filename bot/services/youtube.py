"""Сервис скачивания YouTube — yt-dlp через Cloudflare WARP
WARP = бесплатный VPN, YouTube не блокирует IP Cloudflare.
Cookies не нужны — WARP обходит блокировку датацентровых IP.
Fallback: cookies → ios/android (если WARP упал).
"""
import asyncio
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from typing import Callable

from bot.config import settings

logger = logging.getLogger(__name__)

# лимит файла (Local Bot API — 2 ГБ)
MAX_FILE_SIZE = settings.max_file_size

# WARP SOCKS5 прокси (контейнер warp в docker-compose)
WARP_PROXY = "socks5://warp:9091"


@dataclass
class VideoInfo:
    """Информация о видео (до скачивания)"""
    title: str
    duration: int  # в секундах
    thumbnail: str | None = None
    uploader: str | None = None
    # доступные качества: {"360": 30, "720": 100} (качество → примерный размер в МБ)
    qualities: dict | None = None
    is_live: bool = False


@dataclass
class DownloadResult:
    """Результат скачивания"""
    file_path: str
    media_type: str       # video или audio
    title: str
    duration: int | None = None
    width: int | None = None
    height: int | None = None
    format_key: str = ""  # video_360, video_720, audio


# тип для callback прогресса: (скачано_мб, всего_мб, процент)
ProgressCallback = Callable[[float, float, int], None] | None


class FileTooLargeError(Exception):
    """Файл превышает лимит Telegram (2 ГБ)"""
    pass


def classify_error(error_msg: str) -> str:
    """Классифицирует ошибку yt-dlp в категорию для осмысленных алертов.
    Возвращает: 'geo_blocked', 'unavailable', 'age_restricted', 'ip_blocked',
    'expired_url', 'cookies_expired', 'network', 'unknown'.
    Порядок проверок важен — сначала контентные ошибки, потом инфраструктурные.
    """
    msg = error_msg.lower()

    # 1. блокировка по стране или правообладателем (Content ID: UMG, WMG и т.п.)
    # проверяем ПЕРЕД unavailable — иначе "not available in your country" улетит не туда.
    # цепочку fallback не рвём: другой IP в другой стране может скачать
    if (
        "available in your country" in msg
        or "in your country" in msg
        or "contains claimed content" in msg
        or "have blocked it" in msg
        or "has blocked it" in msg
        or "on copyright grounds" in msg
        or "country_blocked" in msg
    ):
        return "geo_blocked"

    # 2. видео недоступно навсегда — ни один источник не поможет, выходим сразу
    if (
        "video unavailable" in msg
        or "private video" in msg
        or "video is private" in msg
        or "has been removed" in msg
        or "removed by the uploader" in msg
        or "has been terminated" in msg
        or "members-only" in msg
        or "join this channel" in msg
        or "http error 404" in msg
    ):
        return "unavailable"

    # 3. возрастное ограничение — лечится только cookies залогиненного аккаунта.
    # проверяем ПЕРЕД ip_blocked: "sign in to confirm your age" не бот-детект
    if (
        "confirm your age" in msg
        or "age-restricted" in msg
        or "age restricted" in msg
        or "inappropriate for some users" in msg
    ):
        return "age_restricted"

    # 4. YouTube задетектил бота — IP реально забанен, лечится сменой IP.
    # "sign in to confirm you're not a bot" — это именно бот-детект, а не протухшие куки
    # (на WARP/прокси мы вообще без куки ходим, так что "cookies_expired" там невозможен)
    if (
        "not a bot" in msg
        or "sign in to confirm" in msg
        or "detected as a bot" in msg
        or "http error 429" in msg
        or "too many requests" in msg
    ):
        return "ip_blocked"

    # 5. 403 на скачивании сегментов — протухшая подписанная ссылка googlevideo:
    # истёк срок, не решён n-challenge (deno) или нет PO token.
    # Это НЕ бан IP — рестартить WARP бессмысленно (порвём чужие закачки)
    if "403" in msg or "forbidden" in msg:
        return "expired_url"

    # 6. cookies протухли (только когда реально использовались куки)
    if "login required" in msg or "cookies" in msg:
        return "cookies_expired"

    # 7. сеть и транзиентные сбои прокси/CDN (5xx — нода прокси или googlevideo моргнули)
    if (
        "timeout" in msg
        or "timed out" in msg
        or "connection" in msg
        or "unreachable" in msg
        or "socks" in msg
        or "giving up after" in msg
        or "internal server error" in msg
        or "bad gateway" in msg
        or "service unavailable" in msg
        or "http error 5" in msg
    ):
        return "network"

    return "unknown"


class YouTubeDownloader:
    """Скачивает YouTube через yt-dlp + резидентный SOCKS5 прокси (primary) или WARP.
    Fallback chain: primary → fallback → proxy+cookies → proxy+ios/android.
    """

    _COOKIES_PATH = "/app/cookies/cookies.txt"

    def __init__(self):
        self.download_dir = tempfile.mkdtemp(prefix="yt_bot_")
        self._proxy = settings.proxy_url or None
        # SOCKS5 резидентный прокси → primary, WARP → fallback
        self._proxy_first = bool(self._proxy and self._proxy.startswith("socks5://"))
        # callback на падение одного источника — только реакция на инфраструктуру
        # (например рестарт WARP), без сообщений админу. Сигнатура: (source, error) -> None
        self.on_source_failed: Callable[[str, str], None] | None = None
        # callback когда провалилась ВСЯ цепочка fallback — вот тут уже алертим админа.
        # Сигнатура: (failures: list[tuple[source, error]]) -> None
        # оба устанавливаются извне (в main.py) после создания экземпляра
        self.on_all_failed: Callable[[list[tuple[str, str]]], None] | None = None

        if self._proxy_first:
            logger.info("Резидентный SOCKS5 прокси (PRIMARY): %s", self._proxy)
            logger.info("WARP прокси (fallback): %s", WARP_PROXY)
        else:
            logger.info("WARP прокси (PRIMARY): %s", WARP_PROXY)
            if self._proxy:
                logger.info("Резидентный прокси (fallback): %s", self._proxy)

        if self.has_cookies():
            logger.info("Cookies: найдены (fallback)")
        else:
            logger.info("Cookies: не найдены")

    def has_cookies(self) -> bool:
        return os.path.isfile(self._COOKIES_PATH)

    def _fire_source_failed(self, source: str, error: Exception) -> None:
        """Триггер callback'а о падении источника. Не пробрасывает ошибки."""
        if self.on_source_failed is None:
            return
        try:
            self.on_source_failed(source, str(error))
        except Exception as e:
            logger.warning("on_source_failed callback упал: %s", e)

    def _fire_all_failed(self, failures: list[tuple[str, str]]) -> None:
        """Триггер callback'а о полном провале цепочки. Не пробрасывает ошибки."""
        if self.on_all_failed is None or not failures:
            return
        try:
            self.on_all_failed(failures)
        except Exception as e:
            logger.warning("on_all_failed callback упал: %s", e)

    def _cleanup_old_files(self, max_age_minutes: int = 30) -> None:
        now = time.time()
        cutoff = now - max_age_minutes * 60
        try:
            for filename in os.listdir(self.download_dir):
                filepath = os.path.join(self.download_dir, filename)
                if os.path.isfile(filepath) and os.path.getmtime(filepath) < cutoff:
                    os.remove(filepath)
                    logger.info("Очистка старого файла: %s", filename)
        except OSError as e:
            logger.warning("Ошибка при очистке: %s", e)

    def _warp_opts(self) -> dict:
        """Настройки через WARP — все качества без cookies"""
        return {
            "quiet": True,
            "no_warnings": True,
            "proxy": WARP_PROXY,
            # увеличенные таймауты для WARP (SSL может тормозить)
            "socket_timeout": 30,
            "retries": 3,
        }

    def _proxy_opts(self) -> dict:
        """Резидентный SOCKS5 прокси без cookies (primary режим)"""
        return {
            "quiet": True,
            "no_warnings": True,
            "proxy": self._proxy,
            "socket_timeout": 30,
            "retries": 3,
        }

    def _cookies_opts(self) -> dict:
        """Fallback: cookies + WARP"""
        return {
            **self._warp_opts(),
            "cookiefile": self._COOKIES_PATH,
        }

    def _fallback_opts(self) -> dict:
        """Последний шанс: ios/android через WARP"""
        return {
            **self._warp_opts(),
            "extractor_args": {
                "youtube": {
                    "player_client": ["ios", "android"],
                },
            },
        }

    def _proxy_cookies_opts(self) -> dict:
        """Резидентный прокси + cookies (если WARP упал)"""
        opts = {
            "quiet": True,
            "no_warnings": True,
        }
        if self._proxy:
            opts["proxy"] = self._proxy
        opts["cookiefile"] = self._COOKIES_PATH
        return opts

    def _proxy_fallback_opts(self) -> dict:
        """Резидентный прокси + ios/android (если всё упало)"""
        opts = {
            "quiet": True,
            "no_warnings": True,
        }
        if self._proxy:
            opts["proxy"] = self._proxy
        opts["extractor_args"] = {
            "youtube": {
                "player_client": ["ios", "android"],
            },
        }
        return opts

    async def get_info(self, url: str) -> VideoInfo:
        """Получает метаданные видео.
        primary → fallback. Порядок зависит от _proxy_first.
        """
        import yt_dlp

        t_start = time.monotonic()

        # get_info всегда через резидентный SOCKS5 → полный список форматов.
        # На датацентровые IP WARP YouTube отдаёт урезанный список качеств,
        # из-за чего юзер видел разный набор кнопок при повторных запросах.
        # Балансировка остаётся только для скачиваний (download_video/audio),
        # где трафик тяжёлый и экономия на прокси реально важна.
        routing = "proxy_first"

        if self._proxy_first:
            primary_source = "proxy"
            primary_opts = self._proxy_opts()
            fallback_source = "warp"
            fallback_opts = self._warp_opts()
        else:
            primary_source = "warp"
            primary_opts = self._warp_opts()
            fallback_source = "proxy"
            fallback_opts = self._proxy_opts() if self._proxy else None

        ydl_opts = {
            **primary_opts,
            "skip_download": True,
            "ignore_no_formats_error": True,
        }

        loop = asyncio.get_event_loop()
        source = primary_source
        failures: list[tuple[str, str]] = []
        try:
            info = await loop.run_in_executor(
                None, self._extract_info, url, ydl_opts
            )
        except Exception as e:
            # ошибка на стороне контента — fallback'и не помогут
            if classify_error(str(e)) == "unavailable":
                raise
            # инфраструктурный сбой primary — реагируем (рестарт WARP), но админа не дёргаем:
            # fallback ещё может вытянуть, алерт уйдёт только если ляжет вся цепочка
            failures.append((primary_source, str(e)))
            self._fire_source_failed(primary_source, e)
            if fallback_opts:
                logger.warning("%s не дал инфо, пробую %s: %s", primary_source, fallback_source, e)
                source = fallback_source
                fb = {
                    **fallback_opts,
                    "skip_download": True,
                    "ignore_no_formats_error": True,
                }
                try:
                    info = await loop.run_in_executor(
                        None, self._extract_info, url, fb
                    )
                except Exception as e2:
                    if classify_error(str(e2)) != "unavailable":
                        failures.append((fallback_source, str(e2)))
                        self._fire_source_failed(fallback_source, e2)
                    # инфо получить не удалось ничем — вот теперь алертим одним сообщением
                    self._fire_all_failed(failures)
                    raise
            else:
                self._fire_all_failed(failures)
                raise

        elapsed = time.monotonic() - t_start
        logger.info(
            "[METRIC] get_info %.2fs source=%s routing=%s url=%s",
            elapsed, source, routing, url,
        )

        qualities = self._parse_qualities(info)

        return VideoInfo(
            title=info.get("title", "Без названия"),
            duration=info.get("duration", 0),
            thumbnail=info.get("thumbnail"),
            uploader=info.get("uploader"),
            qualities=qualities,
            is_live=bool(info.get("is_live")),
        )

    def _parse_qualities(self, info: dict) -> dict:
        formats = info.get("formats", [])
        duration = info.get("duration", 0) or 0
        target_heights = [360, 480, 720, 1080, 1440]
        result = {}

        audio_size = 0
        for fmt in formats:
            if fmt.get("vcodec", "none") != "none":
                continue
            if fmt.get("acodec", "none") == "none":
                continue
            size = fmt.get("filesize") or fmt.get("filesize_approx") or 0
            if not size and fmt.get("tbr") and duration:
                size = int(fmt["tbr"] * 1000 / 8 * duration)
            if size > audio_size:
                audio_size = size

        for h in target_heights:
            best_size = 0
            for fmt in formats:
                fmt_h = fmt.get("height") or 0
                fmt_w = fmt.get("width") or 0
                short_side = min(fmt_h, fmt_w) if fmt_w else fmt_h
                if short_side != h:
                    continue
                if fmt.get("vcodec", "none") == "none":
                    continue
                size = fmt.get("filesize") or fmt.get("filesize_approx") or 0
                if not size and fmt.get("tbr") and duration:
                    size = int(fmt["tbr"] * 1000 / 8 * duration)
                if size > best_size:
                    best_size = size

            if best_size > 0:
                total = best_size + audio_size
                total_mb = int(total / 1024 / 1024)
                result[str(h)] = max(total_mb, 1)

        if not result:
            result = {"360": 0, "720": 0}

        return result

    async def download_video(
        self, url: str, quality: str = "720",
        progress_callback: ProgressCallback = None,
        prefer_warp: bool = False,
    ) -> DownloadResult:
        """Скачивает видео.
        Порядок попыток:
          - prefer_warp=False (дефолт, большие видео): proxy → warp → proxy+cookies → proxy+ios
          - prefer_warp=True (маленькие видео): warp → proxy → proxy+cookies → proxy+ios
        """
        self._cleanup_old_files()
        t_start = time.monotonic()

        # определяем primary и alt fallback в зависимости от prefer_warp
        use_proxy_primary = self._proxy_first and not prefer_warp
        routing = "hd" if use_proxy_primary else "small"

        # 1. PRIMARY
        if use_proxy_primary:
            primary_opts = self._proxy_opts()
            primary_name = "proxy"
            alt_opts = self._warp_opts()
            alt_name = "warp"
        else:
            primary_opts = self._warp_opts()
            primary_name = "warp"
            alt_opts = self._proxy_opts() if self._proxy else None
            alt_name = "proxy"

        # копим падения по всей цепочке — админу уйдёт один алерт, если ничего не сработало
        failures: list[tuple[str, str]] = []

        try:
            result = await self._download_with_quality(
                url, quality, progress_callback, opts=primary_opts
            )
            checked = self._check_size(result)
            self._log_download_metric("download_video", t_start, primary_name, quality, checked.file_path, routing)
            return checked
        except FileTooLargeError:
            # размер не починится сменой источника — пробрасываем сразу
            raise
        except Exception as e:
            logger.warning("%s не сработал: %s", primary_name, e)
            failures.append((primary_name, str(e)))
            self._fire_source_failed(primary_name, e)
            # ошибка на стороне контента (приват/удалено) — fallback'и не помогут
            if classify_error(str(e)) == "unavailable":
                raise

        # 2. FALLBACK на альтернативный источник (warp ↔ proxy)
        if alt_opts:
            try:
                logger.info("Fallback: %s", alt_name)
                result = await self._download_with_quality(
                    url, quality, progress_callback, opts=alt_opts
                )
                checked = self._check_size(result)
                self._log_download_metric("download_video", t_start, alt_name, quality, checked.file_path, routing)
                return checked
            except FileTooLargeError:
                raise
            except Exception as e:
                logger.warning("%s не сработал: %s", alt_name, e)
                failures.append((alt_name, str(e)))
                self._fire_source_failed(alt_name, e)

        # 3. резидентный прокси + cookies
        if self.has_cookies() and self._proxy:
            try:
                logger.info("Fallback: резидентный прокси + cookies")
                result = await self._download_with_quality(
                    url, quality, progress_callback, opts=self._proxy_cookies_opts()
                )
                checked = self._check_size(result)
                self._log_download_metric("download_video", t_start, "proxy+cookies", quality, checked.file_path, routing)
                return checked
            except FileTooLargeError:
                raise
            except Exception as e:
                logger.warning("Прокси+cookies не сработали: %s", e)
                failures.append(("proxy+cookies", str(e)))
                self._fire_source_failed("proxy+cookies", e)

        # 4. резидентный прокси + ios/android (последний шанс)
        if self._proxy:
            try:
                logger.info("Fallback: резидентный прокси + ios/android")
                result = await self._download_with_quality(
                    url, quality, progress_callback, opts=self._proxy_fallback_opts()
                )
                checked = self._check_size(result)
                self._log_download_metric("download_video", t_start, "proxy+ios", quality, checked.file_path, routing)
                return checked
            except FileTooLargeError:
                raise
            except Exception as e:
                logger.warning("Прокси+ios не сработали: %s", e)
                failures.append(("proxy+ios", str(e)))
                self._fire_source_failed("proxy+ios", e)

        # вся цепочка легла — вот теперь один алерт админу со сводкой
        self._fire_all_failed(failures)
        raise RuntimeError("download_failed")

    def _log_download_metric(
        self, op: str, t_start: float, source: str, quality: str, file_path: str, routing: str = "default",
    ) -> None:
        elapsed = time.monotonic() - t_start
        try:
            size_mb = os.path.getsize(file_path) / 1024 / 1024
        except OSError:
            size_mb = 0
        speed = size_mb / elapsed if elapsed > 0 else 0
        logger.info(
            "[METRIC] %s %.2fs source=%s routing=%s quality=%s size=%.1fMB speed=%.1fMB/s",
            op, elapsed, source, routing, quality, size_mb, speed,
        )

    async def download_audio(
        self, url: str,
        progress_callback: ProgressCallback = None,
        prefer_warp: bool = True,
    ) -> DownloadResult:
        """Скачивает аудио. По умолчанию prefer_warp=True — аудио маленькое,
        разгружаем прокси от мелких файлов."""
        self._cleanup_old_files()
        t_start = time.monotonic()

        use_proxy_primary = self._proxy_first and not prefer_warp
        routing = "hd" if use_proxy_primary else "small"

        # 1. PRIMARY
        if use_proxy_primary:
            primary_opts = self._proxy_opts()
            primary_name = "proxy"
            alt_opts = self._warp_opts()
            alt_name = "warp"
        else:
            primary_opts = self._warp_opts()
            primary_name = "warp"
            alt_opts = self._proxy_opts() if self._proxy else None
            alt_name = "proxy"

        # копим падения по всей цепочке — админу уйдёт один алерт, если ничего не сработало
        failures: list[tuple[str, str]] = []

        try:
            result = await self._do_download_audio(url, progress_callback, opts=primary_opts)
            self._log_download_metric("download_audio", t_start, primary_name, "m4a", result.file_path, routing)
            return result
        except FileTooLargeError:
            # размер не починится сменой источника — пробрасываем сразу, без алерта
            raise
        except Exception as e:
            logger.warning("%s не сработал (аудио): %s", primary_name, e)
            failures.append((primary_name, str(e)))
            self._fire_source_failed(primary_name, e)
            # ошибка на стороне контента (приват/удалено) — fallback'и не помогут
            if classify_error(str(e)) == "unavailable":
                raise

        # 2. FALLBACK на альтернативный источник
        if alt_opts:
            try:
                logger.info("Fallback: %s (аудио)", alt_name)
                result = await self._do_download_audio(url, progress_callback, opts=alt_opts)
                self._log_download_metric("download_audio", t_start, alt_name, "m4a", result.file_path, routing)
                return result
            except FileTooLargeError:
                raise
            except Exception as e:
                logger.warning("%s не сработал (аудио): %s", alt_name, e)
                failures.append((alt_name, str(e)))
                self._fire_source_failed(alt_name, e)

        # 3. резидентный прокси + cookies
        if self.has_cookies() and self._proxy:
            try:
                logger.info("Fallback: прокси + cookies (аудио)")
                result = await self._do_download_audio(url, progress_callback, opts=self._proxy_cookies_opts())
                self._log_download_metric("download_audio", t_start, "proxy+cookies", "m4a", result.file_path, routing)
                return result
            except FileTooLargeError:
                raise
            except Exception as e:
                logger.warning("Прокси+cookies не сработали (аудио): %s", e)
                failures.append(("proxy+cookies", str(e)))
                self._fire_source_failed("proxy+cookies", e)

        # 4. резидентный прокси + ios/android
        if self._proxy:
            try:
                logger.info("Fallback: прокси + ios/android (аудио)")
                result = await self._do_download_audio(url, progress_callback, opts=self._proxy_fallback_opts())
                self._log_download_metric("download_audio", t_start, "proxy+ios", "m4a", result.file_path, routing)
                return result
            except FileTooLargeError:
                raise
            except Exception as e:
                logger.warning("Прокси+ios не сработали (аудио): %s", e)
                failures.append(("proxy+ios", str(e)))
                self._fire_source_failed("proxy+ios", e)

        # вся цепочка легла — вот теперь один алерт админу со сводкой
        self._fire_all_failed(failures)
        raise RuntimeError("download_failed")

    async def _do_download_audio(
        self, url: str, progress_callback: ProgressCallback, opts: dict,
    ) -> DownloadResult:
        """Скачивает аудио в нативном формате (m4a) без перекодирования.
        m4a — нативный формат YouTube (AAC), Telegram играет его как аудио.
        Без FFmpeg postprocessor — экономит до 4 минут CPU на длинных видео.
        """
        import yt_dlp

        output_template = os.path.join(self.download_dir, "%(id)s_audio.%(ext)s")
        ydl_opts = {
            **opts,
            # m4a приоритет; webm/opus как fallback если m4a недоступен
            "format": "bestaudio[ext=m4a]/bestaudio",
            "outtmpl": output_template,
        }

        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(
            None, self._download, url, ydl_opts, progress_callback
        )

        # реальное расширение определяет yt-dlp (обычно m4a, иногда webm)
        actual_ext = info.get("ext", "m4a")
        file_path = self._find_downloaded_file(info, actual_ext)
        if not file_path or not os.path.exists(file_path):
            raise RuntimeError("Не удалось найти скачанный аудиофайл")

        return self._check_size(DownloadResult(
            file_path=file_path,
            media_type="audio",
            title=info.get("title", "YouTube Audio"),
            duration=info.get("duration"),
            format_key="audio",
        ))

    def _check_size(self, result: DownloadResult) -> DownloadResult:
        file_size = os.path.getsize(result.file_path)
        if file_size > MAX_FILE_SIZE:
            self._remove_file(result.file_path)
            raise FileTooLargeError(
                f"Файл слишком большой ({file_size / 1024 / 1024:.0f} МБ)"
            )
        return result

    async def _download_with_quality(
        self, url: str, quality: str,
        progress_callback: ProgressCallback = None,
        opts: dict = None,
    ) -> DownloadResult:
        import yt_dlp

        output_template = os.path.join(self.download_dir, f"%(id)s_{quality}p.%(ext)s")
        height = int(quality)
        format_str = (
            f"bestvideo[height<={height}][vcodec~='^(avc|h264)']+bestaudio[ext=m4a]"
            f"/bestvideo[height<={height}]+bestaudio"
            f"/best[height<={height}]"
            f"/best"
        )

        ydl_opts = {
            **opts,
            "format": format_str,
            "outtmpl": output_template,
            "merge_output_format": "mp4",
        }

        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(
            None, self._download, url, ydl_opts, progress_callback
        )

        file_path = self._find_downloaded_file(info, "mp4")
        if not file_path or not os.path.exists(file_path):
            raise RuntimeError("Не удалось найти скачанный видеофайл")

        return DownloadResult(
            file_path=file_path,
            media_type="video",
            title=info.get("title", "YouTube Video"),
            duration=info.get("duration"),
            width=info.get("width"),
            height=info.get("height"),
            format_key=f"video_{quality}",
        )

    def _extract_info(self, url: str, opts: dict) -> dict:
        import yt_dlp
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    def _download(self, url: str, opts: dict, progress_callback: ProgressCallback = None) -> dict:
        import yt_dlp

        last_update = {"time": 0}

        def _hook(d):
            if d["status"] != "downloading":
                return

            downloaded = d.get("downloaded_bytes", 0)
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)

            # ранний обрыв: не тянем то, что заведомо больше лимита Telegram.
            # проверяем и оценку размера, и уже скачанное — рвём как можно раньше,
            # чтобы не качать все 11 ГБ впустую и только потом отбраковывать.
            if total > MAX_FILE_SIZE or downloaded > MAX_FILE_SIZE:
                biggest = max(total, downloaded)
                raise FileTooLargeError(
                    f"Файл слишком большой ({biggest / 1024 / 1024:.0f} МБ)"
                )

            # прогресс юзеру шлём с троттлингом (лимит Telegram на редактирование)
            if not progress_callback:
                return
            now = time.time()
            if now - last_update["time"] < 3:
                return
            last_update["time"] = now
            if total > 0:
                percent = int(downloaded / total * 100)
                dl_mb = downloaded / 1024 / 1024
                total_mb = total / 1024 / 1024
                progress_callback(dl_mb, total_mb, percent)

        opts["progress_hooks"] = [_hook]

        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=True)

    def _find_downloaded_file(self, info: dict, expected_ext: str) -> str | None:
        video_id = info.get("id", "")
        for filename in os.listdir(self.download_dir):
            if video_id in filename and filename.endswith(f".{expected_ext}"):
                return os.path.join(self.download_dir, filename)
        for filename in sorted(os.listdir(self.download_dir), reverse=True):
            if filename.endswith(f".{expected_ext}"):
                return os.path.join(self.download_dir, filename)
        return None

    def cleanup(self, result: DownloadResult) -> None:
        self._remove_file(result.file_path)

    def _remove_file(self, path: str) -> None:
        try:
            if os.path.exists(path):
                os.remove(path)
                logger.info("Удалён: %s", path)
        except OSError as e:
            logger.warning("Не удалось удалить файл: %s", e)


# глобальный экземпляр
downloader = YouTubeDownloader()
