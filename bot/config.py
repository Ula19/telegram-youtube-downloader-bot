"""Конфигурация бота — все настройки из .env"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # токен бота
    bot_token: str

    # PostgreSQL
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "bot_4_youtube"
    db_user: str = "postgres"
    db_password: str = ""

    # юзернейм бота (для рекламной подписи)
    bot_username: str = ""

    # админы бота (через запятую в .env)
    admin_ids: str = ""
    admin_username: str = "admin"

    # URL Bot API (Local Bot API на VPS = файлы до 2 ГБ)
    bot_api_url: str = "https://api.telegram.org"

    # прокси для YouTube (резидентный IP)
    proxy_url: str = ""

    # ==================== WARP-ПУЛ + PO-TOKEN ====================
    # PO-token провайдер (bgutil) — снимает "Sign in to confirm you're not a bot"
    # без cookies. Пустая строка = PO-token выключен.
    bgutil_base_url: str = "http://bgutil:4416"

    # WARP-пул: несколько контейнеров WARP с разными exit-IP (round-robin + кулдаун).
    # warp_proxies (через запятую) переопределяет автосписок из warp_pool_size.
    warp_proxies: str = ""
    warp_pool_size: int = 5
    # сколько WARP-эндпоинтов (разных IP) перебрать перед уходом на proxy/cookies
    warp_max_tries: int = 3
    # WARP-пул — основной источник (резидентный прокси уходит в конец цепочки).
    # false → вернуть старое поведение (резидентный прокси primary для HD).
    warp_primary: bool = True
    # на сколько секунд выводить WARP-эндпоинт из ротации после ip_blocked
    warp_cooldown_seconds: int = 1800

    # кэш скачива��ий (дни)
    cache_ttl_days: int = 1

    # лимит файла (Local Bot API — 2 ГБ, обычный — 50 МБ)
    max_file_size: int = 2 * 1024 * 1024 * 1024  # 2 ГБ

    # порог размера для балансировки прокси/WARP (МБ)
    # видео < порога скачиваются через WARP (разгружаем прокси от мелких файлов)
    # видео >= порога скачиваются через прокси (быстрый путь для HD)
    small_video_threshold_mb: int = 30

    # префлайт-фильтр: качества с оценкой > этого порога не показываются юзеру
    # (оценка yt-dlp обычно завышена; 2000 — с запасом от лимита 2 ГБ и разгружает сервер от тяжёлых загрузок)
    max_quality_size_mb: int = 2000

    @property
    def admin_id_list(self) -> list[int]:
        """Парсит admin_ids из строки в список int"""
        if not self.admin_ids:
            return []
        return [int(x.strip()) for x in self.admin_ids.split(",") if x.strip()]

    @property
    def warp_proxy_list(self) -> list[str]:
        """Список WARP SOCKS5-эндпоинтов для пула.
        Явный warp_proxies приоритетнее; иначе — warp1..warpN:9091 по имени сервиса.
        """
        if self.warp_proxies.strip():
            return [p.strip() for p in self.warp_proxies.split(",") if p.strip()]
        return [
            f"socks5://warp{i}:9091"
            for i in range(1, max(1, self.warp_pool_size) + 1)
        ]

    @property
    def db_url(self) -> str:
        """URL для подключения к PostgreSQL через asyncpg"""
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",  # игнорируем лишние переменные в .env
    }


# глобальный экземпляр настроек
settings = Settings()
