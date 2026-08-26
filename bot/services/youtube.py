"""Сервис скачивания YouTube — yt-dlp через пул Cloudflare WARP + PO-token.

Архитектура (2026):
- PO-token (bgutil) добавляется во ВСЕ запросы → снимает "Sign in to confirm
  you're not a bot" без cookies.
- WARP-пул: несколько контейнеров WARP с разными exit-IP, round-robin + кулдаун
  "битого" IP. WARP — основной источник (warp_primary=True).
- Резидентный SOCKS5 прокси и cookies уходят в конец цепочки как fallback.

Цепочка попыток (warp_primary): warp → warp(другой IP) → proxy → proxy+cookies → proxy+ios/android.
"""
import asyncio
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Callable

from bot.config import settings

logger = logging.getLogger(__name__)

# лимит файла (Local Bot API — 2 ГБ)
MAX_FILE_SIZE = settings.max_file_size

# запасной WARP-эндпоинт, если пул почему-то пуст
WARP_PROXY = "socks5://warp1:9091"


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
    thumb_path: str | None = None  # превью для Telegram (JPEG <=320px)


# тип для callback прогресса: (скачано_мб, всего_мб, процент)
ProgressCallback = Callable[[float, float, int], None] | None


class FileTooLargeError(Exception):
    """Файл превышает лимит Telegram (2 ГБ)"""
    pass


# ошибки на стороне контента: fallback'и не помогут, цепочку обрываем сразу и молчим
_CONTENT_CATEGORIES = {"unavailable", "geo_blocked"}


def classify_error(error_msg: str) -> str:
    """Классифицирует ошибку yt-dlp в категорию для осмысленных алертов.
    Возвращает: 'geo_blocked', 'unavailable', 'age_restricted', 'ip_blocked',
    'no_formats', 'expired_url', 'cookies_expired', 'network', 'unknown'.
    Порядок проверок важен — сначала контентные ошибки, потом инфраструктурные.
    """
    msg = error_msg.lower()

    # 1. блокировка по стране или правообладателем (Content ID: UMG, WMG и т.п.)
    # проверяем ПЕРЕД unavailable — иначе "not available in your country" улетит не туда
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

    # 2. видео недоступно навсегда — ни один источник не поможет, выходим сразу.
    # сюда же завершённые трансляции: у них форматов уже нет, перебирать цепочку незачем
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
        or "live event has ended" in msg
        or "live event will begin" in msg
        or "premieres in" in msg
        or "this live stream recording is not available" in msg
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

    # 4. YouTube задетектил бота — IP реально забанен, лечится сменой IP (ротацией пула WARP).
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

    # 5. yt-dlp не смог извлечь форматы: список пустой, любой селектор мимо.
    # Обычно ломается JS-рантайм (deno/n-challenge) или PO-token — это НЕ про IP.
    # Симптом одинаков для видео и аудио и повторяется на всех источниках сразу
    if (
        "requested format is not available" in msg
        or "no video formats found" in msg
        or "unable to extract player" in msg
        or "failed to extract any player response" in msg
        or "nsig extraction failed" in msg
        or "no supported javascript runtime" in msg
    ):
        return "no_formats"

    # 6. 403 на скачивании сегментов — протухшая подписанная ссылка googlevideo:
    # истёк срок, не решён n-challenge (deno) или не сработал PO-token.
    # Это НЕ бан IP: эндпоинт в кулдаун не отправляем и контейнер не рестартим
    if "403" in msg or "forbidden" in msg:
        return "expired_url"

    # 7. cookies протухли (только когда реально использовались куки)
    if "login required" in msg or "cookies" in msg:
        return "cookies_expired"

    # 8. сеть и транзиентные сбои прокси/CDN (5xx — нода прокси или googlevideo моргнули)
    if (
        "timeout" in msg
        or "timed out" in msg
        or "connection" in msg
        or "unreachable" in msg
        or "socks" in msg
        or "giving up after" in msg
        # мёртвый SOCKS-эндпоинт: контейнер не резолвится или порт не слушает
        or "name resolution" in msg
        or "getaddrinfo" in msg
        or "errno -3" in msg
        or "errno 111" in msg
        or "refused" in msg
        or "internal server error" in msg
        or "bad gateway" in msg
        or "service unavailable" in msg
        or "http error 5" in msg
    ):
        return "network"

    return "unknown"


class _YdlLogger:
    """Прокидывает сообщения yt-dlp в лог бота.

    В опциях стоят quiet/no_warnings (чтобы yt-dlp не сорил в stdout), но они же
    глушат важные предупреждения — "No supported JavaScript runtime" (сломан deno)
    и проблемы с PO-token. Без них в логах остаётся только финальное
    "Requested format is not available", по которому причину не найти.
    """

    def debug(self, msg: str) -> None:
        pass  # отладка yt-dlp слишком шумная

    def info(self, msg: str) -> None:
        pass

    def warning(self, msg: str) -> None:
        logger.warning("yt-dlp: %s", msg)

    def error(self, msg: str) -> None:
        logger.error("yt-dlp: %s", msg)


_YDL_LOGGER = _YdlLogger()


class WarpPool:
    """Round-robin по WARP-эндпоинтам с кулдауном упавших (ip_blocked).

    Каждый WARP-контейнер имеет свой exit-IP; если YouTube заблокировал один IP,
    выводим его из ротации на cooldown и продолжаем работать через остальные.
    """

    def __init__(self, proxies: list[str], cooldown_seconds: int):
        self._proxies = list(proxies)
        self._cooldown = max(0, cooldown_seconds)
        self._idx = 0
        self._blocked_until: dict[str, float] = {}
        self._lock = threading.Lock()

    def all(self) -> list[str]:
        return list(self._proxies)

    def pick(self) -> str | None:
        """Следующий доступный эндпоинт (round-robin). Если все на кулдауне —
        берём тот, у кого кулдаун закончится раньше всех.
        """
        with self._lock:
            if not self._proxies:
                return None
            now = time.time()
            n = len(self._proxies)
            for _ in range(n):
                url = self._proxies[self._idx % n]
                self._idx = (self._idx + 1) % n
                if self._blocked_until.get(url, 0.0) <= now:
                    return url
            # все на кулдауне — наименее "просроченный"
            return min(self._proxies, key=lambda u: self._blocked_until.get(u, 0.0))

    def mark_blocked(self, url: str | None) -> None:
        if not url:
            return
        with self._lock:
            if url in self._proxies:
                self._blocked_until[url] = time.time() + self._cooldown

    def is_member(self, url: str | None) -> bool:
        return bool(url) and url in self._proxies


class YouTubeDownloader:
    """Скачивает YouTube через yt-dlp: пул WARP (primary) + PO-token,
    резидентный SOCKS5 прокси и cookies — как fallback.
    """

    _COOKIES_PATH = "/app/cookies/cookies.txt"

    def __init__(self):
        self.download_dir = tempfile.mkdtemp(prefix="yt_bot_")
        self._proxy = settings.proxy_url or None
        self._proxy_is_socks = bool(self._proxy and self._proxy.startswith("socks5://"))
        # WARP-пул — основной источник; резидентный прокси уходит в конец цепочки
        self._warp_primary = settings.warp_primary
        self._warp_pool = WarpPool(settings.warp_proxy_list, settings.warp_cooldown_seconds)
        # callback на падение одной попытки — только реакция на инфраструктуру
        # (рестарт WARP-контейнера), без сообщений админу.
        # сигнатура: (source: str, error: str, endpoint: str | None) -> None; ставится извне (main.py)
        self.on_source_failed: Callable[..., None] | None = None
        # callback когда провалилась ВСЯ цепочка попыток — вот тут уже алертим админа.
        # сигнатура: (failures: list[tuple[source, error]]) -> None
        self.on_all_failed: Callable[[list[tuple[str, str]]], None] | None = None

        pool = self._warp_pool.all()
        logger.info("WARP-пул (%d эндпоинтов, primary=%s): %s",
                    len(pool), self._warp_primary, ", ".join(pool) or "—")
        if self._proxy:
            role = "fallback" if self._warp_primary else "primary"
            logger.info("Резидентный прокси (%s): %s", role, self._proxy)
        if settings.bgutil_base_url:
            logger.info("PO-token (bgutil): %s", settings.bgutil_base_url)
        else:
            logger.info("PO-token: выключен")
        logger.info("Cookies: %s", "найдены (fallback)" if self.has_cookies() else "не найдены")

    def has_cookies(self) -> bool:
        return os.path.isfile(self._COOKIES_PATH)

    async def check_pot_provider(self) -> None:
        """Best-effort проверка доступности PO-token провайдера (bgutil) при старте.
        Ничего не ломает — только логирует: PO-token опционален, при недоступности
        провайдера yt-dlp качает без токена (с риском бот-детекта). Нужна потому,
        что quiet=True глушит родное предупреждение yt-dlp о недоступном POT.
        """
        base = (settings.bgutil_base_url or "").strip()
        if not base:
            return
        url = base.rstrip("/") + "/ping"

        def _ping() -> int:
            import urllib.request
            import urllib.error
            try:
                with urllib.request.urlopen(url, timeout=3) as r:
                    return getattr(r, "status", 200) or 200
            except urllib.error.HTTPError as he:
                return he.code  # сервер ответил (пусть даже 404) — значит доступен

        loop = asyncio.get_event_loop()
        for attempt in range(3):
            try:
                status = await loop.run_in_executor(None, _ping)
                logger.info("PO-token провайдер доступен (%s, HTTP %s)", url, status)
                return
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2)
                    continue
                logger.warning(
                    "PO-token провайдер НЕдоступен (%s): %s — качаем без токена", url, e,
                )

    def _fire_source_failed(self, source: str, error: Exception, endpoint: str | None = None) -> None:
        """Триггер callback'а о падении источника. Не пробрасывает ошибки.
        endpoint — конкретный WARP-эндпоинт (для точечного рестарта его контейнера).
        """
        if self.on_source_failed is None:
            return
        try:
            self.on_source_failed(source, str(error), endpoint)
        except Exception as e:
            logger.warning("on_source_failed callback упал: %s", e)

    def _fire_all_failed(self, failures: list[tuple[str, str]]) -> None:
        """Триггер callback'а о провале всей цепочки. Не пробрасывает ошибки."""
        if self.on_all_failed is None or not failures:
            return
        try:
            self.on_all_failed(failures)
        except Exception as e:
            logger.warning("on_all_failed callback упал: %s", e)

    def _note_attempt_failure(self, name: str, opts: dict, error: Exception) -> None:
        """Обрабатывает падение одной попытки: реакция на инфраструктуру + кулдаун эндпоинта.
        Админу тут НЕ пишем — дальше по цепочке ещё может сработать fallback.
        - если попытка шла через WARP-эндпоинт из пула — передаём его в callback (для рестарта);
        - при ip_blocked через WARP выводим этот эндпоинт из ротации на cooldown.
          403 (expired_url) сюда не попадает: это протухшая ссылка, а не бан IP
        """
        endpoint = opts.get("proxy") if isinstance(opts, dict) else None
        is_warp = self._warp_pool.is_member(endpoint)
        self._fire_source_failed(name, error, endpoint if is_warp else None)
        if is_warp and classify_error(str(error)) == "ip_blocked":
            logger.warning(
                "WARP-эндпоинт %s заблокирован YouTube → кулдаун %dс",
                endpoint, settings.warp_cooldown_seconds,
            )
            self._warp_pool.mark_blocked(endpoint)

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

    # ---------- PO-token ----------

    async def check_js_runtime(self) -> None:
        """Best-effort проверка JS-рантайма (deno) при старте — только лог.

        Свежие версии yt-dlp без JS-рантайма не могут решить n-challenge и часто
        возвращают ПУСТОЙ список форматов. Наружу это выглядит как
        "Requested format is not available" — одинаково для видео и аудио и сразу
        на всех источниках. Проверяем явно, чтобы это было видно в логах при старте.
        """
        def _probe() -> str:
            import subprocess
            out = subprocess.run(
                ["deno", "--version"], capture_output=True, text=True, timeout=10,
            )
            return (out.stdout or out.stderr or "").strip().splitlines()[0] if out.stdout or out.stderr else ""

        loop = asyncio.get_event_loop()
        try:
            version = await loop.run_in_executor(None, _probe)
            logger.info("JS-рантайм (deno) доступен: %s", version or "версия не определена")
        except FileNotFoundError:
            logger.error(
                "JS-рантайм (deno) НЕ НАЙДЕН — yt-dlp не решит n-challenge и вернёт "
                "пустой список форматов ('Requested format is not available'). "
                "Проверь установку deno в Dockerfile",
            )
        except Exception as e:
            logger.error("JS-рантайм (deno) не отвечает: %s", e)

    def _pot_args(self) -> dict:
        """extractor_args для PO-token провайдера (bgutil). Пусто, если выключен."""
        base = (settings.bgutil_base_url or "").strip()
        if not base:
            return {}
        return {"youtubepot-bgutilhttp": {"base_url": [base]}}

    def _with_pot(self, opts: dict, youtube_args: dict | None = None) -> dict:
        """Добавляет PO-token (и опционально youtube-параметры вроде player_client)
        в extractor_args, не затирая уже существующие ключи.
        """
        ea = dict(opts.get("extractor_args") or {})
        ea.update(self._pot_args())
        if youtube_args:
            ea["youtube"] = {**ea.get("youtube", {}), **youtube_args}
        if ea:
            opts["extractor_args"] = ea
        return opts

    # ---------- наборы опций yt-dlp ----------

    def _warp_opts(self) -> dict:
        """WARP из пула (следующий доступный exit-IP) + PO-token."""
        proxy = self._warp_pool.pick() or WARP_PROXY
        return self._with_pot({
            "quiet": True,
            "no_warnings": True,
            "proxy": proxy,
            # увеличенные таймауты для WARP (SSL может тормозить)
            "socket_timeout": 30,
            "retries": 3,
        })

    def _proxy_opts(self) -> dict:
        """Резидентный SOCKS5 прокси без cookies + PO-token."""
        return self._with_pot({
            "quiet": True,
            "no_warnings": True,
            "proxy": self._proxy,
            "socket_timeout": 30,
            "retries": 3,
        })

    def _proxy_cookies_opts(self) -> dict:
        """Резидентный прокси + cookies + PO-token."""
        opts = {"quiet": True, "no_warnings": True}
        if self._proxy:
            opts["proxy"] = self._proxy
        opts["cookiefile"] = self._COOKIES_PATH
        return self._with_pot(opts)

    def _proxy_fallback_opts(self) -> dict:
        """Резидентный прокси + ios/android клиенты + PO-token (последний шанс)."""
        opts = {"quiet": True, "no_warnings": True}
        if self._proxy:
            opts["proxy"] = self._proxy
        return self._with_pot(opts, youtube_args={"player_client": ["ios", "android"]})

    def _warp_cookies_opts(self) -> dict:
        """WARP из пула + cookies + PO-token (cookies-fallback без резидентного прокси)."""
        opts = self._warp_opts()
        opts["cookiefile"] = self._COOKIES_PATH
        return opts

    def _warp_fallback_opts(self) -> dict:
        """WARP из пула + ios/android клиенты + PO-token (последний шанс без прокси)."""
        return self._with_pot(self._warp_opts(), youtube_args={"player_client": ["ios", "android"]})

    # ---------- построение цепочки попыток ----------

    def _attempts(self, prefer_warp: bool) -> list[tuple[str, Callable[[], dict]]]:
        """Упорядоченная цепочка попыток (name, thunk-опций).

        thunk вызывается в момент попытки → WARP выбирает свежий IP с учётом
        кулдаунов, выставленных предыдущими попытками.
        """
        # резидентный прокси primary только при явном warp_primary=False и SOCKS5-прокси
        proxy_primary = (not self._warp_primary) and self._proxy_is_socks and not prefer_warp

        pool_n = len(self._warp_pool.all())
        warp_tries = min(settings.warp_max_tries, pool_n) if pool_n else 1
        warp_steps: list[tuple[str, Callable[[], dict]]] = [
            ("warp", self._warp_opts) for _ in range(warp_tries)
        ]
        proxy_step: list[tuple[str, Callable[[], dict]]] = (
            [("proxy", self._proxy_opts)] if self._proxy else []
        )

        attempts: list[tuple[str, Callable[[], dict]]] = []
        if proxy_primary:
            attempts += proxy_step + warp_steps
        else:
            attempts += warp_steps + proxy_step

        # хвост: cookies-fallback (через прокси, иначе через WARP), затем ios/android
        if self.has_cookies():
            if self._proxy:
                attempts.append(("proxy+cookies", self._proxy_cookies_opts))
            else:
                attempts.append(("warp+cookies", self._warp_cookies_opts))
        if self._proxy:
            attempts.append(("proxy+ios", self._proxy_fallback_opts))
        else:
            attempts.append(("warp+ios", self._warp_fallback_opts))
        return attempts

    async def _run_download_attempts(
        self, op: str, t_start: float, quality_label: str, routing: str,
        attempts: list[tuple[str, Callable[[], dict]]],
        run_fn: Callable[[dict], "asyncio.Future"],
    ) -> DownloadResult:
        """Перебирает попытки до первого успеха.
        - FileTooLargeError не триггерит fallback (пробрасывается сразу).
        - unavailable (приват/удалено) и geo_blocked (страна/копирайт) — early-exit
          без fallback и без алертов: перебирать весь пул на них бессмысленно.
        - на ip_blocked через WARP-эндпоинт ставит его в кулдаун.
        - падения копятся: админу уйдёт ОДИН алерт, если не сработало вообще ничего.
        """
        last_err: Exception | None = None
        failures: list[tuple[str, str]] = []
        for name, thunk in attempts:
            opts = thunk()
            try:
                result = await run_fn(opts)
                checked = self._check_size(result)
                self._log_download_metric(op, t_start, name, quality_label, checked.file_path, routing)
                return checked
            except FileTooLargeError:
                raise
            except Exception as e:
                last_err = e
                if classify_error(str(e)) in _CONTENT_CATEGORIES:
                    raise
                logger.warning("%s не сработал (%s): %s", name, op, e)
                failures.append((name, str(e)))
                self._note_attempt_failure(name, opts, e)
        # не сработала ни одна попытка — вот теперь один алерт со сводкой
        self._fire_all_failed(failures)
        if last_err:
            raise last_err
        raise RuntimeError("download_failed")

    # ---------- публичные методы ----------

    async def get_info(self, url: str) -> VideoInfo:
        """Получает метаданные видео (лёгкий запрос — только форматы).
        Если задан резидентный SOCKS5-прокси, идём через него первым: на датацентровых
        IP WARP YouTube отдаёт УРЕЗАННЫЙ список качеств, из-за чего юзер видел разный
        набор кнопок. Без прокси — через WARP-пул (PO-token частично компенсирует).
        Таймауты здесь короче, чем при скачивании: метаданные лёгкие, а долгий get_info
        задерживает показ кнопок качества.
        """
        t_start = time.monotonic()

        # WARP+PO-token отдаёт ПОЛНЫЙ список качеств (проверено yt-dlp -F: DASH 240..1080).
        # Поэтому при включённом bgutil идём WARP-пулом первым — именно резидентный
        # прокси на датацентровом/забаненном IP обрезал список до 360/720.
        # Без PO-token оставляем старую логику (proxy-first), т.к. голый WARP урезает.
        pot_on = bool(self._pot_args())
        warp_first = pot_on or not self._proxy_is_socks
        routing = "warp_first" if warp_first else "proxy_first"

        pool_n = len(self._warp_pool.all())
        warp_tries = min(2, pool_n) if pool_n else 1
        warp_steps: list[tuple[str, Callable[[], dict]]] = [
            ("warp", self._warp_opts) for _ in range(warp_tries)
        ]
        proxy_step: list[tuple[str, Callable[[], dict]]] = (
            [("proxy", self._proxy_opts)] if self._proxy else []
        )
        attempts = warp_steps + proxy_step if warp_first else proxy_step + warp_steps

        loop = asyncio.get_event_loop()
        info = None
        source = ""
        last_err: Exception | None = None
        failures: list[tuple[str, str]] = []
        for name, thunk in attempts:
            opts = {
                **thunk(),
                "skip_download": True,
                "ignore_no_formats_error": True,
                # метаданные лёгкие — не ждём по 90с на медленном эндпоинте
                "socket_timeout": 15,
                "retries": 1,
            }
            try:
                info = await loop.run_in_executor(None, self._extract_info, url, opts)
                source = name
                break
            except Exception as e:
                last_err = e
                # ошибка на стороне контента (приват/гео-блок) — fallback'и не помогут
                if classify_error(str(e)) in _CONTENT_CATEGORIES:
                    raise
                logger.warning("%s не дал инфо: %s", name, e)
                failures.append((name, str(e)))
                self._note_attempt_failure(name, opts, e)

        if info is None:
            # инфо не дал никто — один алерт со сводкой
            self._fire_all_failed(failures)
            raise last_err if last_err else RuntimeError("get_info_failed")

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
        По умолчанию (warp_primary=True) цепочка:
          warp → warp(другой IP) → proxy → proxy+cookies → proxy+ios.
        prefer_warp сохранён для обратной совместимости (влияет только при warp_primary=False).
        """
        self._cleanup_old_files()
        t_start = time.monotonic()

        attempts = self._attempts(prefer_warp)
        proxy_primary = (not self._warp_primary) and self._proxy_is_socks and not prefer_warp
        routing = "proxy_first" if proxy_primary else "warp_first"

        async def run_fn(opts: dict) -> DownloadResult:
            return await self._download_with_quality(url, quality, progress_callback, opts=opts)

        return await self._run_download_attempts(
            "download_video", t_start, quality, routing, attempts, run_fn,
        )

    async def download_audio(
        self, url: str,
        progress_callback: ProgressCallback = None,
        prefer_warp: bool = True,
    ) -> DownloadResult:
        """Скачивает аудио. prefer_warp=True — аудио маленькое, идёт через WARP-пул."""
        self._cleanup_old_files()
        t_start = time.monotonic()

        attempts = self._attempts(prefer_warp)
        proxy_primary = (not self._warp_primary) and self._proxy_is_socks and not prefer_warp
        routing = "proxy_first" if proxy_primary else "warp_first"

        async def run_fn(opts: dict) -> DownloadResult:
            return await self._do_download_audio(url, progress_callback, opts=opts)

        return await self._run_download_attempts(
            "download_audio", t_start, "m4a", routing, attempts, run_fn,
        )

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

    async def _do_download_audio(
        self, url: str, progress_callback: ProgressCallback, opts: dict,
    ) -> DownloadResult:
        """Скачивает аудио в нативном формате (m4a) без перекодирования.
        m4a — нативный формат YouTube (AAC), Telegram играет его как аудио.
        Без FFmpeg postprocessor — экономит до 4 минут CPU на длинных видео.
        Размер проверяет вызывающий (_run_download_attempts).
        """
        output_template = os.path.join(self.download_dir, "%(id)s_audio.%(ext)s")
        ydl_opts = {
            **opts,
            # m4a приоритет; webm/opus как fallback если m4a недоступен
            "format": "bestaudio[ext=m4a]/bestaudio",
            "outtmpl": output_template,
            # обложка ролика — у аудио своего кадра нет, а без превью
            # Telegram рисует пустой квадрат
            "writethumbnail": True,
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

        return DownloadResult(
            file_path=file_path,
            media_type="audio",
            title=info.get("title", "YouTube Audio"),
            duration=info.get("duration"),
            format_key="audio",
            thumb_path=self.build_thumb(file_path, allow_frame=False),
        )

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
        output_template = os.path.join(self.download_dir, f"%(id)s_{quality}p.%(ext)s")
        height = int(quality)
        # YouTube помечает качество по КОРОТКОЙ стороне (720p шортса = 720x1280).
        # По height фильтровать нельзя: у вертикальных видео height — это длинная
        # сторона, и height<=720 роняет 720p-шортс до 360p. Ограничиваем ДЛИННУЮ
        # сторону (обе стороны <= 2*target) — так нужный тир корректно отделяется
        # и в landscape, и в портрете (тиры YouTube по длинной стороне разнесены >2x).
        cap = height * 2
        format_str = (
            f"bestvideo[height<={cap}][width<={cap}][vcodec~='^(avc|h264)']+bestaudio[ext=m4a]"
            f"/bestvideo[height<={cap}][width<={cap}]+bestaudio"
            f"/best[height<={cap}][width<={cap}]"
            # хвост без ограничений: если под cap не нашлось ничего (например, видео
            # выложено только в 4K), лучше отдать доступное качество, чем уронить
            # скачивание в "Requested format is not available"
            f"/bestvideo+bestaudio"
            f"/best"
        )

        ydl_opts = {
            **opts,
            "format": format_str,
            "outtmpl": output_template,
            "merge_output_format": "mp4",
            # обложка ролика — основной источник превью (кадр из видео бывает чёрным)
            "writethumbnail": True,
        }

        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(
            None, self._download, url, ydl_opts, progress_callback
        )

        file_path = self._find_downloaded_file(info, "mp4")
        if not file_path or not os.path.exists(file_path):
            raise RuntimeError("Не удалось найти скачанный видеофайл")

        duration = info.get("duration")
        return DownloadResult(
            file_path=file_path,
            media_type="video",
            title=info.get("title", "YouTube Video"),
            duration=duration,
            width=info.get("width"),
            height=info.get("height"),
            format_key=f"video_{quality}",
            thumb_path=self.build_thumb(file_path, duration),
        )

    def _extract_info(self, url: str, opts: dict) -> dict:
        import yt_dlp
        opts.setdefault("logger", _YDL_LOGGER)
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    def _download(self, url: str, opts: dict, progress_callback: ProgressCallback = None) -> dict:
        import yt_dlp

        opts.setdefault("logger", _YDL_LOGGER)
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

    # ---------- превью для Telegram ----------

    # Telegram принимает превью только как JPEG со сторонами <= 320px и весом <= 200 КБ.
    # Без него он берёт ПЕРВЫЙ кадр видео, а он часто чёрный (фейд, заставка) —
    # в чате получался чёрный прямоугольник вместо обложки.
    _THUMB_MAX_SIDE = 320
    _THUMB_MAX_BYTES = 200 * 1024
    _THUMB_SCALE = "scale=w=320:h=320:force_original_aspect_ratio=decrease"

    def _run_ffmpeg(self, args: list[str], timeout: int = 30) -> bool:
        """Запускает ffmpeg. Возвращает True при успехе. Ошибки не пробрасывает —
        превью необязательно, без него отправка всё равно должна пройти.
        """
        import subprocess
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error", *args],
                capture_output=True, timeout=timeout, check=True,
            )
            return True
        except Exception as e:
            logger.warning("ffmpeg не отработал (%s): %s", args[-1], e)
            return False

    def build_thumb(self, media_path: str, duration: int | None = None,
                    allow_frame: bool = True) -> str | None:
        """Готовит превью для уже скачанного файла.

        ВАЖНО: превью — косметика. Файл к этому моменту скачан, поэтому любая
        ошибка здесь гасится: отправим без превью, но отправим. Юзер ошибки не увидит.

        Порядок: обложка ролика (её кладёт yt-dlp рядом) → кадр из самого видео
        (сеть уже не нужна). allow_frame=False для аудио — там кадра нет.
        """
        try:
            thumb = self._find_downloaded_thumb(media_path)
            if thumb is None and allow_frame:
                thumb = self.make_video_thumb(media_path, duration)
            return thumb
        except Exception as e:
            logger.warning("Превью не получилось (%s) — отправляем без него", e)
            return None

    def _finalize_thumb(self, path: str) -> str | None:
        """Проверяет, что превью получилось и влезает в лимит Telegram."""
        if not os.path.isfile(path):
            return None
        size = os.path.getsize(path)
        if 0 < size <= self._THUMB_MAX_BYTES:
            return path
        logger.warning("Превью не влезло в лимит (%d байт) — отправим без него", size)
        self._remove_file(path)
        return None

    # ниже этой средней яркости кадр считаем чёрным (0..255)
    _THUMB_MIN_BRIGHTNESS = 16.0

    def _frame_brightness(self, path: str) -> float:
        """Средняя яркость картинки. -1, если измерить не вышло."""
        import subprocess
        try:
            out = subprocess.run(
                ["ffmpeg", "-i", path, "-vf",
                 "signalstats,metadata=print:key=lavfi.signalstats.YAVG",
                 "-f", "null", "-"],
                capture_output=True, text=True, timeout=20,
            ).stderr
            for line in out.splitlines():
                if "YAVG" in line:
                    return float(line.split("=")[-1])
        except Exception:
            pass
        return -1.0

    def make_video_thumb(self, video_path: str, duration: int | None) -> str | None:
        """Кадр из видео — фолбэк, когда обложку ролика скачать не удалось.

        Первый кадр брать нельзя: у роликов начало часто чёрное (фейд, заставка) —
        именно поэтому в чате был чёрный прямоугольник. Пробуем несколько моментов
        и берём первый, который не оказался чёрным.
        """
        thumb = os.path.splitext(video_path)[0] + "_thumb.jpg"
        if duration and duration > 4:
            offsets = [min(max(duration * f, 2.0), 600.0) for f in (0.1, 0.3, 0.5)]
        else:
            offsets = [1.0, 0.0]

        # старый файл с прошлого скачивания мог остаться — иначе рискуем отдать
        # его как «своё» превью, даже если ffmpeg сейчас не отработал
        self._remove_file(thumb)

        produced = False
        for offset in offsets:
            ok = self._run_ffmpeg([
                "-ss", f"{offset:.1f}", "-i", video_path,
                "-frames:v", "1", "-vf", self._THUMB_SCALE, "-q:v", "5", thumb,
            ])
            if not ok or not os.path.isfile(thumb):
                continue
            produced = True
            brightness = self._frame_brightness(thumb)
            if brightness < 0 or brightness >= self._THUMB_MIN_BRIGHTNESS:
                return self._finalize_thumb(thumb)
            logger.info("Кадр на %.1fs чёрный (яркость %.1f) — пробую позже", offset, brightness)

        # все кадры тёмные — отдаём последний, он всё равно лучше пустого квадрата
        return self._finalize_thumb(thumb) if produced else None

    def _convert_thumb(self, src: str, dst: str) -> str | None:
        """Обложку с YouTube (webp/png) → JPEG нужного размера."""
        self._remove_file(dst)  # не выдаём за результат файл с прошлого раза
        ok = self._run_ffmpeg(["-i", src, "-vf", self._THUMB_SCALE, "-q:v", "5", dst])
        return self._finalize_thumb(dst) if ok else None

    def _find_downloaded_thumb(self, media_path: str) -> str | None:
        """Ищет обложку ролика, скачанную yt-dlp рядом с файлом (writethumbnail),
        и приводит её к формату Telegram.

        Это предпочтительный источник превью: обложка совпадает с той, что юзер
        видел на YouTube, и она гарантированно не чёрная — в отличие от кадра из
        начала видео.
        """
        base = os.path.splitext(media_path)[0]
        dst = f"{base}_thumb.jpg"
        thumb = None
        for ext in ("jpg", "jpeg", "png", "webp"):
            src = f"{base}.{ext}"
            if not os.path.isfile(src):
                continue
            # если формат не сконвертировался (например, нет декодера webp) —
            # не сдаёмся, пробуем следующий файл
            if thumb is None:
                thumb = self._convert_thumb(src, dst)
            self._remove_file(src)  # оригинал больше не нужен
        return thumb

    def cleanup(self, result: DownloadResult) -> None:
        self._remove_file(result.file_path)
        if result.thumb_path:
            self._remove_file(result.thumb_path)

    def _remove_file(self, path: str) -> None:
        try:
            if os.path.exists(path):
                os.remove(path)
                logger.info("Удалён: %s", path)
        except OSError as e:
            logger.warning("Не удалось удалить файл: %s", e)


# глобальный экземпляр
downloader = YouTubeDownloader()
