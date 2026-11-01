#!/usr/bin/env python3
"""Мостик «облачные камеры А.М.Видео / ЕГДС → локальный RTSP».

Зачем: Frigate и другие NVR работают только с локальными потоками,
а камеры А.М.Видео (MPEG1-TS over WebSocket, jsmpeg) и ЕГДС
(JPEG-снимки по HTTPS с Basic-авторизацией) — облачные. Мостик логинится
в сервисах сам и переливает картинку в RTSP внутри домашней сети
(по умолчанию — в mediamtx, из которого Frigate и берёт поток).

Протоколы восстановлены при разработке интеграций amvideo-ha и egds-ha:

  А.М.Видео (lk.amvideo-msk.ru, api4.php):
    POST type=login  (login, password, device_key)  → sid
    POST type=video  (id_shlag, sid, typeDevice)    → {wssport: ["7077/TOKEN"]}
    WSS wss://stream.amvideo-msk.ru:{порт}/{путь}   → бинарный MPEG1-TS
    (TLS видеопрокси не проверяется — сертификат выдан на другой хост;
     keep-alive сессии: type=auth раз в 55 минут)

  ЕГДС (revXX.egdsk.ru:11013, rsapi):
    GET /rsapi/snapshot?id={Guid}  — JPEG-кадр,
    Basic {Login}:{Password} камеры, TLS самоподписанный → ssl=False.
    Логин/пароль камеры — статические, получаются один раз скриптом
    get_egds_camera_creds.py.

Запуск:  python main.py [/путь/config.yaml]   (по умолчанию /config/config.yaml)

Мостик не зависит от Home Assistant и Frigate.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import uuid

import aiohttp
import yaml

log = logging.getLogger("bridge")

APP_VERSION = "3.4.1"
KEEPALIVE_INTERVAL = 3300        # type=auth раз в 55 минут (как в приложении)
RECONNECT_DELAY = 5.0            # пауза между попытками переподключения
SNAPSHOT_ERROR_DELAY = 5.0       # пауза после ошибки снимка ЕГДС
SNAPSHOT_AUTH_DELAY = 30.0       # пауза после 401 (неверные учётные данные)
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30)
SNAPSHOT_TIMEOUT = aiohttp.ClientTimeout(total=10)

AMVIDEO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
    "AMVideoApp/3.4.1",
    "X-Requested-With": "ru.moscow.shlagbaum",
}

JPEG_SOI = b"\xff\xd8\xff"


class FatalSourceError(Exception):
    """Ошибка, при которой источник останавливается (нужен PIN и т.п.)."""


def digits(value: str) -> str:
    return "".join(ch for ch in str(value) if ch.isdigit())


def build_ffmpeg_args(
    source_type: str,
    out_url: str,
    fps: float,
    width: int | None,
) -> list[str]:
    """Собрать командную строку ffmpeg: вход с stdin → H.264 → RTSP.

    amvideo: вход — MPEG1-TS (бинарный поток WSS).
    egds:    вход — конкатенация JPEG-кадров (image2pipe).
    Кодирование x264 veryfast/zerolatency — работает на CPU без GPU.
    Выход — push в RTSP-сервер (mediamtx из docker-compose).
    """
    args = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
    if source_type == "amvideo":
        args += ["-fflags", "nobuffer", "-flags", "low_delay"]
        args += ["-f", "mpegts", "-i", "pipe:0"]
    else:
        args += ["-f", "image2pipe", "-framerate", str(fps), "-i", "pipe:0"]
    args += [
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-tune", "zerolatency",
        "-pix_fmt", "yuv420p",
    ]
    if width:
        args += ["-vf", f"scale={width}:-2"]
    args += [
        "-r", str(fps),
        "-g", str(int(fps) * 2),
        "-b:v", "1000k",
        "-f", "rtsp",
        "-rtsp_transport", "tcp",
        out_url,
    ]
    return args


async def drain_stderr(source_id: str, stderr: asyncio.StreamReader) -> None:
    """Собирать stderr ffmpeg в журнал (не даём пайпу переполниться)."""
    while True:
        line = await stderr.readline()
        if not line:
            return
        text = line.decode(errors="replace").strip()
        if text:
            log.debug("[%s] ffmpeg: %s", source_id, text)


class AmVideoSource:
    """Источник А.М.Видео: WSS с MPEG1-TS → stdin ffmpeg."""

    def __init__(self, cfg: dict, http: aiohttp.ClientSession) -> None:
        self.id = cfg["id"]
        self.http = http
        self.base_url = cfg.get(
            "base_url", "https://lk.amvideo-msk.ru/api/api4.php"
        )
        self.stream_host = cfg.get(
            "stream_host", "wss://stream.amvideo-msk.ru:{port}"
        )
        self.login = digits(cfg["login"])
        self.password = str(cfg["password"])
        self.id_shlag = cfg.get("id_shlag")
        self.sid: str | None = None
        self._keepalive_task: asyncio.Task | None = None

    async def _post(self, payload: dict) -> dict:
        async with self.http.post(
            self.base_url, data=payload, headers=AMVIDEO_HEADERS, timeout=HTTP_TIMEOUT
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} от {self.base_url}")
            return await resp.json(content_type=None)

    async def _login(self) -> None:
        data = await self._post(
            {
                "type": "login",
                "login": self.login,
                "password": self.password,
                "device_key": str(uuid.uuid4()),
                "pin": "",
            }
        )
        if data.get("result") and data.get("sid"):
            self.sid = data["sid"]
            log.info("[%s] вход выполнен (sid=…%s)", self.id, str(self.sid)[-4:])
            return
        if data.get("change_device"):
            raise FatalSourceError(
                "сервер требует PIN-код устройства (вход с нового устройства). "
                "Выполните первый вход с приложением/интеграцией и повторите."
            )
        raise RuntimeError(f"Ошибка входа: {data.get('str') or 'неверный логин/пароль'}")

    async def _keepalive_loop(self) -> None:
        """type=auth раз в 55 минут — как в приложении; сессию не перелогинивает."""
        while True:
            await asyncio.sleep(KEEPALIVE_INTERVAL)
            if not self.sid:
                continue
            try:
                result = await self._post(
                    {
                        "type": "auth",
                        "sid": self.sid,
                        "with_redirect": "true",
                        "app_version": APP_VERSION,
                    }
                )
                if not result.get("result"):
                    log.warning(
                        "[%s] сессия истекла (type=auth), перелогин при следующем подключении",
                        self.id,
                    )
                    self.sid = None
            except Exception as err:  # noqa: BLE001
                log.debug("[%s] keep-alive не удался: %s", self.id, err)

    async def _video_url(self) -> str:
        """type=video → цель потока («порт» или «порт/токен») → WSS URL."""
        id_shlag = self.id_shlag
        if not id_shlag:
            panels = await self._post({"type": "panels", "sid": self.sid})
            if not panels.get("result"):
                raise RuntimeError(f"panels: {panels.get('str')}")
            with_cam = [p for p in (panels.get("data") or []) if p.get("cam")]
            if not with_cam:
                raise RuntimeError("нет доступных КПУ с камерой")
            id_shlag = with_cam[0]["id_shlag"]
            log.info("[%s] id_shlag не задан, выбран первый КПУ: %s", self.id, id_shlag)
        result = await self._post(
            {
                "type": "video",
                "id_shlag": id_shlag,
                "sid": self.sid,
                "typeDevice": "panel",
            }
        )
        if result.get("status") == "ok" and result.get("wssport"):
            target = str(result["wssport"][0]).strip()
            if "://" in target:
                return target
            return self.stream_host.format(port=target.lstrip("/"))
        raise RuntimeError(str(result.get("str") or "сервер не выдал порт видео"))

    async def pump(self, stdin: asyncio.StreamWriter) -> None:
        """Подключиться и качать MPEG-TS в stdin ffmpeg (до разрыва)."""
        if not self.sid:
            await self._login()
            if self._keepalive_task is None or self._keepalive_task.done():
                self._keepalive_task = asyncio.create_task(self._keepalive_loop())
        url = await self._video_url()
        log.info("[%s] подключение к %s", self.id, url.split("?")[0])
        async with self.http.ws_connect(url, ssl=False) as ws:
            async for msg in ws:
                if msg.type is aiohttp.WSMsgType.BINARY:
                    stdin.write(msg.data)
                    await stdin.drain()
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.ERROR,
                ):
                    break
                elif msg.type is aiohttp.WSMsgType.TEXT:
                    log.debug("[%s] WSS text: %s", self.id, msg.data[:120])


class EgdsSource:
    """Источник ЕГДС: JPEG-снимки rsapi → stdin ffmpeg (image2pipe)."""

    def __init__(self, cfg: dict, http: aiohttp.ClientSession) -> None:
        self.id = cfg["id"]
        self.http = http
        self.auth = aiohttp.BasicAuth(str(cfg["login"]), str(cfg["password"]))
        self.url = (
            f"https://{cfg['host']}:{cfg['port']}"
            f"/rsapi/snapshot?id={cfg['guid']}"
        )
        self.interval = 1.0 / float(cfg.get("framerate", 2))

    async def pump(self, stdin: asyncio.StreamWriter) -> None:
        """Качать снимки: 200/JPEG → в пайп; 401 → предупредить и притормозить."""
        while True:
            try:
                async with self.http.get(
                    self.url, auth=self.auth, ssl=False, timeout=SNAPSHOT_TIMEOUT
                ) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        if data[:3] == JPEG_SOI:
                            stdin.write(data)
                            await stdin.drain()
                        else:
                            log.warning(
                                "[%s] сервер вернул не JPEG (%d байт)",
                                self.id,
                                len(data),
                            )
                    elif resp.status == 401:
                        log.error(
                            "[%s] 401: проверьте login/password камеры в config.yaml "
                            "(пароль должен быть расшифрованным, см. "
                            "get_egds_camera_creds.py)",
                            self.id,
                        )
                        await asyncio.sleep(SNAPSHOT_AUTH_DELAY)
                    else:
                        log.warning("[%s] HTTP %s от камеры", self.id, resp.status)
                        await asyncio.sleep(SNAPSHOT_ERROR_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                log.warning("[%s] снимок не получен: %s", self.id, err)
                await asyncio.sleep(SNAPSHOT_ERROR_DELAY)
            await asyncio.sleep(self.interval)


def make_source(cfg: dict, http: aiohttp.ClientSession):
    kind = str(cfg.get("type", "")).lower()
    if kind == "amvideo":
        return AmVideoSource(cfg, http)
    if kind == "egds":
        return EgdsSource(cfg, http)
    raise ValueError(f"источник {cfg.get('id')}: неизвестный тип '{kind}'")


async def run_source(cfg: dict, http: aiohttp.ClientSession) -> None:
    """Жизненный цикл одного источника: ffmpeg + перекачка + переподключение."""
    src = make_source(cfg, http)
    out_url = cfg.get("rtsp_url") or cfg["_out_url"]
    fps = float(cfg.get("fps", cfg.get("framerate", 5)))
    width = cfg.get("width")

    while True:
        args = build_ffmpeg_args(cfg["type"], out_url, fps, width)
        log.info("[%s] запуск ffmpeg → %s", src.id, out_url)
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            log.critical("[%s] ffmpeg не найден в системе", src.id)
            return
        stderr_task = asyncio.create_task(drain_stderr(src.id, proc.stderr))
        try:
            await src.pump(proc.stdin)
        except FatalSourceError as err:
            log.critical("[%s] остановлен: %s", src.id, err)
            stderr_task.cancel()
            return
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001
            log.warning("[%s] поток прервался: %s", src.id, err)
        finally:
            try:
                proc.stdin.close()
            except (OSError, RuntimeError):
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
            stderr_task.cancel()
        await asyncio.sleep(RECONNECT_DELAY)


def load_config(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    outputs = (data or {}).get("outputs") or {}
    rtsp = outputs.get("rtsp") or {}
    rtsp_host = rtsp.get("host", "mediamtx")
    rtsp_port = int(rtsp.get("port", 8554))
    sources = []
    for cfg in (data or {}).get("sources") or []:
        if not cfg.get("enabled", True):
            log.info("источник %s отключён (enabled: false)", cfg.get("id"))
            continue
        cfg = dict(cfg)
        cfg["_out_url"] = cfg.get("rtsp_url") or (
            f"rtsp://{rtsp_host}:{rtsp_port}/{cfg['id']}"
        )
        sources.append(cfg)
    if not sources:
        raise SystemExit("В config.yaml нет ни одного включённого источника")
    return sources


async def main() -> None:
    logging.basicConfig(
        level=os.environ.get("BRIDGE_LOGLEVEL", "info").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/config/config.yaml"
    if not os.path.exists(config_path):
        raise SystemExit(f"Конфигурация не найдена: {config_path}")
    sources = load_config(config_path)
    stop = asyncio.Event()

    def _sig(*_args) -> None:
        stop.set()

    for signame in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(signame, _sig)
        except NotImplementedError:  # Windows
            signal.signal(signame, _sig)

    async with aiohttp.ClientSession() as http:
        tasks = [asyncio.create_task(run_source(cfg, http)) for cfg in sources]
        await stop.wait()
        log.info("Остановка мостика…")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
