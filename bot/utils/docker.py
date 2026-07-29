"""Управление Docker-контейнерами через Unix socket"""
import asyncio
import json
import logging
import time
from urllib.parse import quote

logger = logging.getLogger(__name__)

DOCKER_SOCKET = "/var/run/docker.sock"

# троттлинг: не чаще 1 рестарта в 5 минут — на КАЖДЫЙ сервис отдельно
_last_restart: dict[str, float] = {}
_RESTART_COOLDOWN = 300


async def restart_warp(service: str = "warp") -> bool:
    """Перезапускает WARP-контейнер compose-сервиса `service` (warp1..warpN) для смены IP.
    Возвращает True если рестарт выполнен, False если на кулдауне или ошибка.
    Кулдаун считается отдельно по каждому сервису.
    """
    now = time.time()
    last = _last_restart.get(service, 0.0)
    if now - last < _RESTART_COOLDOWN:
        logger.info(
            "WARP рестарт %s на кулдауне (осталось %d сек)",
            service, int(_RESTART_COOLDOWN - (now - last)),
        )
        return False

    try:
        # находим container ID по label compose-сервиса
        flt = quote(json.dumps({"label": [f"com.docker.compose.service={service}"]}))
        find_cmd = (
            'curl -s --unix-socket /var/run/docker.sock '
            f'"http://localhost/containers/json?filters={flt}"'
        )
        proc = await asyncio.create_subprocess_shell(
            find_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()

        containers = json.loads(stdout)
        if not containers:
            logger.warning("WARP контейнер сервиса %s не найден через Docker API", service)
            return False

        container_id = containers[0]["Id"][:12]
        logger.info("Перезапуск WARP %s (%s) для смены IP...", service, container_id)

        # рестарт контейнера (timeout=10 сек на graceful stop)
        restart_cmd = (
            f'curl -s --unix-socket /var/run/docker.sock '
            f'-X POST "http://localhost/containers/{container_id}/restart?t=10"'
        )
        proc = await asyncio.create_subprocess_shell(
            restart_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()

        if proc.returncode == 0:
            _last_restart[service] = time.time()
            logger.info("WARP %s (%s) перезапущен", service, container_id)
            return True
        else:
            logger.warning("Не удалось перезапустить WARP %s: returncode=%d", service, proc.returncode)
            return False

    except Exception as e:
        logger.warning("Ошибка при перезапуске WARP %s: %s", service, e)
        return False
