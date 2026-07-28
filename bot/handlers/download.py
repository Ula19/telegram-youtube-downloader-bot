"""Хэндлер скачивания — обрабатывает ссылки YouTube
Флоу: ссылка → выбор формата → выбор качества → скачивание → отправка
"""
import asyncio
import html
import logging
import os
import time

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, Message

from bot.database import async_session
from bot.database.crud import (
    get_cached_download,
    get_or_create_user,
    get_user_language,
    save_download,
)
from bot.i18n import t
from bot.keyboards.inline import (
    get_audio_suggest_keyboard,
    get_back_keyboard,
    get_format_keyboard,
    get_quality_keyboard,
)
from bot.services.youtube import FileTooLargeError, classify_error, downloader
from bot.utils.helpers import clean_youtube_url, is_youtube_url
from bot.config import settings
from bot.emojis import E

logger = logging.getLogger(__name__)
router = Router()

# минимальный интервал обновления прогресса (Telegram лимит ~30 ред/мин)
PROGRESS_UPDATE_INTERVAL = 4

# троттлинг алертов о fallback (одно сообщение раз в N секунд)
_FALLBACK_ALERT_THROTTLE = 600  # 10 минут
_last_fallback_alert: dict[str, float] = {}

# человеко-понятные подписи к категориям ошибок
_ERROR_CATEGORY_LABELS = {
    "cookies_expired": "Cookies протухли — обнови через /update_cookies",
    "ip_blocked": "YouTube заблокировал IP — нужна ротация прокси",
    "expired_url": "Протухшая ссылка на видео (403) — n-challenge или PO token",
    "age_restricted": "Возрастное ограничение — нужны cookies залогиненного аккаунта",
    "network": "Сетевая ошибка (таймаут или 5xx у прокси/CDN)",
    "geo_blocked": "Видео заблокировано по стране или правообладателем",
    "unavailable": "Видео недоступно (приват/удалено)",
    "unknown": "Неизвестная ошибка",
}

# категории которые не алертим админу — это ошибки на стороне контента, не инфраструктуры
_SILENT_CATEGORIES = {"unavailable", "geo_blocked"}

# что важнее показать админу, когда упало несколько источников с разными ошибками
_CATEGORY_PRIORITY = [
    "ip_blocked",
    "cookies_expired",
    "network",
    "expired_url",
    "unknown",
    "age_restricted",
    "geo_blocked",
    "unavailable",
]

# максимум 30 одновременных скачиваний — сервер мощный (8ГБ ОЗУ, 6.4ГБ свободно)
_download_semaphore = asyncio.Semaphore(30)


def _make_progress_bar(percent: int, dl_mb: float, total_mb: float) -> str:
    """Рисует полоску прогресса"""
    filled = int(percent / 100 * 12)
    bar = "▰" * filled + "▱" * (12 - filled)
    return (
        f"{E['clock']} Скачиваю...\n"
        f"{bar} {percent}%\n"
        f"{dl_mb:.0f} МБ из {total_mb:.0f} МБ"
    )


# FSM для сохранения URL между шагами выбора
class DownloadStates(StatesGroup):
    waiting_format = State()
    waiting_quality = State()


@router.message(F.text)
async def handle_youtube_link(message: Message, state: FSMContext) -> None:
    """Обработка текстовых сообщений — ищем ссылки YouTube"""
    text = message.text.strip()

    async with async_session() as session:
        lang = await get_user_language(session, message.from_user.id)

    # проверяем что это ссылка на YouTube
    if not is_youtube_url(text):
        await message.answer(
            t("download.not_youtube", lang),
            parse_mode="HTML",
        )
        return

    clean_url = clean_youtube_url(text)

    # получаем инфо о видео
    try:
        status_msg = await message.answer(t("download.fetching_info", lang))
        info = await downloader.get_info(clean_url)

        # прямой эфир — скачивание не поддерживается
        if info.is_live:
            await status_msg.edit_text(
                t("error.live_stream", lang),
                parse_mode="HTML",
            )
            return

        # префлайт-фильтр: выкидываем качества с оценкой > лимита
        # (оценка yt-dlp обычно завышена, порог с запасом)
        # размер 0 = неизвестно → оставляем, юзер сам разберётся
        max_mb = settings.max_quality_size_mb
        filtered_qualities = {
            q: size for q, size in (info.qualities or {}).items()
            if size == 0 or size <= max_mb
        }

        # все качества оказались слишком большими → предлагаем только аудио
        if info.qualities and not filtered_qualities:
            await state.set_state(DownloadStates.waiting_format)
            await state.update_data(url=clean_url)
            await status_msg.edit_text(
                t("error.too_large_suggest_audio", lang),
                reply_markup=get_audio_suggest_keyboard(lang),
                parse_mode="HTML",
            )
            return

        # сохраняем URL и инфо в FSM
        await state.set_state(DownloadStates.waiting_format)
        await state.update_data(
            url=clean_url,
            title=info.title,
            duration=info.duration,
            qualities=filtered_qualities,
            msg_id=message.message_id,
        )

        # форматируем длительность
        duration_str = _format_duration(info.duration)

        await status_msg.edit_text(
            t("download.info", lang,
              title=info.title,
              duration=duration_str,
              uploader=info.uploader or "—"),
            reply_markup=get_format_keyboard(lang),
            parse_mode="HTML",
        )

    except Exception as e:
        logger.error(f"Ошибка получения инфо: {e}")
        error_text = _get_error_text(str(e), lang)
        if status_msg:
            await status_msg.edit_text(error_text)
        else:
            await message.answer(error_text)


@router.callback_query(F.data == "fmt_video")
async def choose_video_format(callback: CallbackQuery, state: FSMContext) -> None:
    """Юзер выбрал видео — показываем качество с размерами"""
    async with async_session() as session:
        lang = await get_user_language(session, callback.from_user.id)

    data = await state.get_data()
    qualities = data.get("qualities")
    await state.set_state(DownloadStates.waiting_quality)

    await callback.message.edit_text(
        t("download.choose_quality", lang),
        reply_markup=get_quality_keyboard(lang, qualities),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "fmt_audio")
async def download_audio(callback: CallbackQuery, state: FSMContext) -> None:
    """Юзер выбрал аудио — скачиваем m4a"""
    data = await state.get_data()
    url = data.get("url")
    await state.clear()

    # отвечаем на callback СРАЗУ (Telegram даёт 30 сек)
    await callback.answer()

    if not url:
        await callback.message.answer(f"{E['cross']} Ссылка не найдена, отправь заново")
        return

    async with async_session() as session:
        lang = await get_user_language(session, callback.from_user.id)

    await _process_download(
        callback.message, url, "audio", callback.from_user, lang, state
    )


@router.callback_query(F.data.startswith("quality_"))
async def choose_quality(callback: CallbackQuery, state: FSMContext) -> None:
    """Юзер выбрал качество — скачиваем видео"""
    quality = callback.data.replace("quality_", "")  # "360" или "720"
    data = await state.get_data()
    url = data.get("url")
    qualities = data.get("qualities") or {}
    expected_size_mb = int(qualities.get(quality, 0) or 0)
    await state.clear()

    # отвечаем на callback СРАЗУ (Telegram даёт 30 сек)
    await callback.answer()

    if not url:
        await callback.message.answer(f"{E['cross']} Ссылка не найдена, отправь заново")
        return

    async with async_session() as session:
        lang = await get_user_language(session, callback.from_user.id)

    format_key = f"video_{quality}"
    await _process_download(
        callback.message, url, format_key, callback.from_user, lang, state,
        expected_size_mb=expected_size_mb,
        qualities=qualities,
    )


async def _process_download(
    message: Message,
    url: str,
    format_key: str,
    user,
    lang: str = "ru",
    state: FSMContext | None = None,
    expected_size_mb: int = 0,
    qualities: dict | None = None,
) -> None:
    """Скачивает и отправляет медиа.
    expected_size_mb используется для балансировки прокси/WARP:
    маленькие видео идут через WARP, большие — через прокси.
    qualities нужны чтобы при FileTooLargeError предложить меньшее качество.
    """
    # проверяем кэш
    async with async_session() as session:
        await get_or_create_user(
            session=session,
            telegram_id=user.id,
            username=user.username,
            full_name=user.full_name,
        )
        cached = await get_cached_download(session, url, format_key)

    if cached:
        logger.info(f"Кэш найден для {url} [{format_key}]")
        await _send_cached(message, cached.file_id, cached.media_type)
        return

    async with _download_semaphore:
        # скачиваем
        status_msg = await message.edit_text(t("download.processing", lang))

        # callback для обновления прогресса
        last_progress_update = {"time": 0}
        # захватываем loop ДО executor (внутри потока get_event_loop() не работает)
        loop = asyncio.get_event_loop()

        def on_progress(dl_mb: float, total_mb: float, percent: int):
            """yt-dlp вызывает это из другого потока, шедулим обновление в asyncio"""
            now = time.time()
            if now - last_progress_update["time"] < PROGRESS_UPDATE_INTERVAL:
                return
            last_progress_update["time"] = now

            text = _make_progress_bar(percent, dl_mb, total_mb)
            # шедулим в event loop (хук вызывается из другого потока)
            try:
                asyncio.run_coroutine_threadsafe(
                    _safe_edit(status_msg, text), loop
                )
            except Exception:
                pass

        result = None
        try:
            if format_key == "audio":
                # аудио всегда маленькое — через WARP
                result = await downloader.download_audio(url, on_progress, prefer_warp=True)
            else:
                quality = format_key.replace("video_", "")
                # балансировка по размеру: маленькие → WARP, большие → прокси
                prefer_warp = 0 < expected_size_mb < settings.small_video_threshold_mb
                result = await downloader.download_video(
                    url, quality, on_progress, prefer_warp=prefer_warp,
                )

            file_id = await _send_media(message, result, status_msg, lang)

            # сохраняем в кэш
            if file_id:
                actual_format_key = result.format_key or format_key
                async with async_session() as session:
                    await save_download(
                        session=session,
                        youtube_url=url,
                        format_key=actual_format_key,
                        file_id=file_id,
                        media_type=result.media_type,
                    )
                    user_obj = await get_or_create_user(
                        session=session,
                        telegram_id=user.id,
                        username=user.username,
                        full_name=user.full_name,
                    )
                    user_obj.download_count += 1
                    await session.commit()

            # удаляем статусное сообщение
            try:
                await status_msg.delete()
            except Exception:
                pass

        except FileTooLargeError:
            # файл превысил 2 ГБ — предлагаем качества пониже, если они есть
            current_quality = format_key.replace("video_", "") if format_key.startswith("video_") else None
            lower_qualities = {}
            if current_quality and qualities:
                try:
                    current_h = int(current_quality)
                    lower_qualities = {
                        q: size for q, size in qualities.items()
                        if int(q) < current_h
                    }
                except ValueError:
                    pass

            if lower_qualities:
                # есть качества пониже — показываем их
                await status_msg.edit_text(
                    t("error.too_large_try_lower", lang),
                    reply_markup=get_quality_keyboard(lang, lower_qualities),
                    parse_mode="HTML",
                )
                if state:
                    await state.set_state(DownloadStates.waiting_quality)
                    await state.update_data(url=url, qualities=lower_qualities)
            else:
                # качеств пониже нет — предлагаем аудио
                await status_msg.edit_text(
                    t("error.too_large_suggest_audio", lang),
                    reply_markup=get_audio_suggest_keyboard(lang),
                    parse_mode="HTML",
                )
                if state:
                    await state.set_state(DownloadStates.waiting_format)
                    await state.update_data(url=url)

        except Exception as e:
            logger.error(f"Ошибка скачивания {url}: {e}")
            error_text = _get_error_text(str(e), lang)
            try:
                await status_msg.edit_text(error_text)
            except Exception:
                await message.answer(error_text)

        finally:
            if result:
                downloader.cleanup(result)



async def _send_media(message: Message, result, status_msg=None, lang="ru") -> str | None:
    """Отправляет медиа юзеру и возвращает file_id.
    Через Local Bot API — файлы до 2 ГБ без ограничений.
    """
    file = FSInputFile(result.file_path)

    # Уведомляем пользователя перед долгой отправкой
    if status_msg:
        try:
            await status_msg.edit_text(t("download.uploading", lang))
        except Exception:
            pass

    t_upload = time.monotonic()
    try:
        size_mb = os.path.getsize(result.file_path) / 1024 / 1024
    except OSError:
        size_mb = 0

    if result.media_type == "video":
        promo = t("download.promo", lang, bot_username=settings.bot_username)
        sent = await message.answer_video(
            video=file,
            caption=f"{E['video']} {result.title}{promo}",
            duration=int(result.duration) if result.duration else None,
            width=result.width,
            height=result.height,
        )
        _log_upload_metric("video", t_upload, size_mb)
        return sent.video.file_id

    elif result.media_type == "audio":
        promo = t("download.promo", lang, bot_username=settings.bot_username)
        sent = await message.answer_audio(
            audio=file,
            caption=f"{E['audio']} {result.title}{promo}",
            duration=int(result.duration) if result.duration else None,
            title=result.title,
        )
        _log_upload_metric("audio", t_upload, size_mb)
        return sent.audio.file_id

    return None


def _log_upload_metric(media_type: str, t_start: float, size_mb: float) -> None:
    elapsed = time.monotonic() - t_start
    speed = size_mb / elapsed if elapsed > 0 else 0
    logger.info(
        "[METRIC] upload_%s %.2fs size=%.1fMB speed=%.1fMB/s",
        media_type, elapsed, size_mb, speed,
    )


async def _send_cached(
    message: Message, file_id: str, media_type: str
) -> None:
    """Отправляет из кэша по file_id"""
    try:
        if media_type == "video":
            await message.answer_video(video=file_id, caption=f"{E['video']} YouTube Video")
        elif media_type == "audio":
            await message.answer_audio(audio=file_id, caption=f"{E['audio']} YouTube Audio")
    except Exception as e:
        logger.error(f"Ошибка отправки из кэша: {e}")
        await message.answer(f"{E['warning']} Кэш устарел. Отправь ссылку ещё раз.")


async def _safe_edit(msg: Message, text: str) -> None:
    """Безопасно обновляет сообщение (игнорирует ошибки лимита Telegram)"""
    try:
        await msg.edit_text(text)
    except Exception:
        pass


def _format_duration(seconds: int) -> str:
    """Форматирует секунды в MM:SS или HH:MM:SS"""
    if not seconds:
        return "—"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def _get_error_text(error: str, lang: str = "ru") -> str:
    """Человеко-понятное сообщение об ошибке"""
    error_lower = error.lower()

    if "private" in error_lower or "login" in error_lower:
        return t("error.private", lang)
    elif "not found" in error_lower or "404" in error_lower:
        return t("error.not_found", lang)
    elif "unavailable" in error_lower:
        return t("error.unavailable", lang)
    elif "too large" in error_lower or "50 мб" in error_lower:
        return t("error.too_large", lang)
    elif "timeout" in error_lower:
        return t("error.timeout", lang)
    elif "available in your country" in error_lower:
        return t("error.geo_blocked", lang)
    elif "age" in error_lower:
        return t("error.age_restricted", lang)
    else:
        return t("error.generic", lang)


# bot instance — устанавливается из main.py через setup_fallback_alerts
_bot_ref = None


def setup_fallback_alerts(bot) -> None:
    """Подключает callbacks к downloader: реакция на падение источника + алерты админу.
    Вызывается из main.py после создания бота.
    """
    global _bot_ref
    _bot_ref = bot
    downloader.on_source_failed = _on_source_failed
    downloader.on_all_failed = _on_all_failed
    logger.info("Алерты о падении источников подключены")


def _on_source_failed(source: str, error: str) -> None:
    """Падение одного источника. Админу НЕ пишем — дальше по цепочке может сработать fallback.
    Здесь только лог и техническая реакция на инфраструктуру.
    """
    category = classify_error(error)
    logger.warning("Источник %s упал (%s): %s", source, category, error[:200])

    # WARP забанен YouTube — молча перезапускаем для смены IP (у restart_warp свой кулдаун)
    if source == "warp" and category == "ip_blocked":
        try:
            asyncio.create_task(_restart_warp_silently())
        except RuntimeError:
            pass  # нет активного event loop


async def _restart_warp_silently() -> None:
    """Перезапуск WARP без уведомления админа — только запись в лог."""
    from bot.utils.docker import restart_warp
    restarted = await restart_warp()
    if restarted:
        logger.info("WARP перезапущен после ip_blocked (без алерта)")


def _on_all_failed(failures: list[tuple[str, str]]) -> None:
    """Sync callback: провалилась вся цепочка источников. Шедулит один алерт админу."""
    if _bot_ref is None or not failures:
        return
    try:
        asyncio.create_task(_send_failure_alert(failures))
    except RuntimeError:
        # нет активного event loop — игнорируем
        pass


async def _send_failure_alert(failures: list[tuple[str, str]]) -> None:
    """Один алерт админу со сводкой по всей провалившейся цепочке. С троттлингом."""
    # классифицируем каждое падение и выбираем главную категорию по приоритету
    classified = [(src, err, classify_error(err)) for src, err in failures]
    categories = {c for _, _, c in classified}
    main_category = next(
        (c for c in _CATEGORY_PRIORITY if c in categories),
        "unknown",
    )

    # если хоть один источник сказал "контент недоступен" (гео-блок, копирайт, приват) —
    # видео и не скачается ничем, а бот-детект на другом IP тут вторичен. Админу знать незачем
    content_issue = categories & _SILENT_CATEGORIES
    if content_issue:
        logger.info(
            "Скачивание провалилось по контенту (%s) — алерт не шлём",
            ", ".join(sorted(content_issue)),
        )
        return

    now = time.time()
    last = _last_fallback_alert.get(main_category, 0)
    if now - last < _FALLBACK_ALERT_THROTTLE:
        return
    _last_fallback_alert[main_category] = now

    # сводка по источникам: что именно вернул каждый
    lines = "\n".join(
        f"• <b>{src}</b> — {_ERROR_CATEGORY_LABELS.get(cat, cat)}"
        for src, _, cat in classified
    )
    # текст последней ошибки — по ней обычно и понятно, что чинить
    # экранируем: в ошибках бывают URL с & и <, Telegram на них ругается и не шлёт сообщение
    last_error = classified[-1][1]
    short_error = last_error[:300] + "..." if len(last_error) > 300 else last_error
    short_error = html.escape(short_error)

    text = (
        f"{E['warning']} <b>Скачивание провалилось</b>\n"
        f"<i>Не сработал ни один источник</i>\n\n"
        f"{lines}\n\n"
        f"<b>Последняя ошибка:</b>\n<code>{short_error}</code>"
    )

    for admin_id in settings.admin_id_list:
        try:
            await _bot_ref.send_message(admin_id, text, parse_mode="HTML")
            logger.info("Админ %s уведомлён о полном провале цепочки (%s)", admin_id, main_category)
        except Exception as e:
            logger.warning("Не удалось уведомить админа %s: %s", admin_id, e)

