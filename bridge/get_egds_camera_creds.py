#!/usr/bin/env python3
"""Одноразовый помощник: получить учётные данные камер ЕГДС для мостика.

Пароли камер rsapi статические, но сервер ЕГДС отдаёт их зашифрованными
и только после входа. Скрипт делает то же, что приложение ЕГДС:

  1. просит SMS-код (grant_type=sms);
  2. спрашивает код у вас (stdin);
  3. получает токены (grant_type=password);
  4. читает /mobile1/barrier/addresses (Bearer-токен);
  5. расшифровывает пароли камер (AES-256-CBC, ключ PBKDF2 от
     "OurSecretKey#1" — восстановлено из EgdsMaui.dll 3.3.25);
  6. печатает готовый блок YAML для bridge/config.yaml.

Запуск (на любой машине с python3, где доступен client.egdsk.ru):

  pip install aiohttp cryptography
  python get_egds_camera_creds.py 79991234567

Код доступа — тот же, что для входа в приложение ЕГДС. Скрипт НЕ хранит
токены и НЕ отправляет данные куда-либо, кроме серверов ЕГДС.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import sys
from getpass import getpass

import aiohttp

CLIENT_ID = "AndroidApp_1.3"
CLIENT_SECRET = "Secret#1"
URL_OAUTH = "https://client.egdsk.ru/oauth/token"
URL_ADDRESSES = "https://client.egdsk.ru/mobile1/barrier/addresses"

# --- дешифрование пароля камеры (из EgdsMaui.dll 3.3.25) -----------------
CAM_KEY_PASSWORD = b"OurSecretKey#1"
CAM_KEY_SALT = bytes.fromhex("45344e0d394a5613")
CAM_KEY_ITERATIONS = 300
CAM_KEY_LENGTH = 32

BASIC = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()


def decrypt_camera_password(stored: str | None) -> str | None:
    """Расшифровать "b64(данные);b64(IV)" в открытый пароль камеры."""
    if not stored or ";" not in stored:
        return stored
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.primitives.padding import PKCS7

        data_b64, iv_b64 = stored.split(";", 1)
        key = hashlib.pbkdf2_hmac(
            "sha1", CAM_KEY_PASSWORD, CAM_KEY_SALT, CAM_KEY_ITERATIONS, CAM_KEY_LENGTH
        )
        decryptor = Cipher(
            algorithms.AES(key), modes.CBC(base64.b64decode(iv_b64))
        ).decryptor()
        padded = decryptor.update(base64.b64decode(data_b64)) + decryptor.finalize()
        unpadder = PKCS7(128).unpadder()
        return (unpadder.update(padded) + unpadder.finalize()).decode("utf-8")
    except Exception:  # noqa: BLE001 — не расшифровалось: вернуть как есть
        return stored


def norm_phone(phone: str) -> str:
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) == 10 and digits[0] != "7":
        digits = "7" + digits
    if len(digits) == 11 and digits[0] == "8":
        digits = "7" + digits[1:]
    return digits


async def main() -> int:
    parser = argparse.ArgumentParser(description="Учётные данные камер ЕГДС")
    parser.add_argument("phone", help="телефон аккаунта ЕГДС")
    parser.add_argument("--code", help="код из SMS (иначе спросит в консоли)")
    args = parser.parse_args()

    phone = norm_phone(args.phone)
    async with aiohttp.ClientSession() as http:
        headers = {"Authorization": f"Basic {BASIC}"}

        async with http.post(
            URL_OAUTH,
            data={"grant_type": "sms", "phone_number": phone},
            headers=headers,
        ) as resp:
            result = await resp.json(content_type=None)
        if not result.get("access_token") and "verification_method" in str(
            result.get("error", "")
        ):
            print(
                "ЕГДС не может доставить код: не подключён ни один канал "
                "(SMS/звонок, бот MAX, Telegram, email).\n"
                "Подключите канал в приложении ЕГДС (Настройки уведомлений) "
                "или в lk.egdsk.ru и повторите.",
                file=sys.stderr,
            )
            return 1
        print(f"Код входа отправлен на {phone} (SMS/звонок).")

        code = args.code or getpass("Введите код из SMS: ")
        async with http.post(
            URL_OAUTH,
            data={
                "grant_type": "password",
                "username": phone,
                "password": code,
            },
            headers=headers,
        ) as resp:
            tokens = await resp.json(content_type=None)
        access = tokens.get("access_token")
        if not access:
            print(f"Не удалось получить токен: {tokens}", file=sys.stderr)
            return 1

        async with http.get(
            URL_ADDRESSES, headers={"Authorization": f"Bearer {access}"}
        ) as resp:
            payload = await resp.json(content_type=None)

    rows = []
    addresses = payload if isinstance(payload, list) else payload.get("data") or []
    for addr in addresses:
        for barrier in addr.get("Barriers") or addr.get("barriers") or []:
            bar_name = " / ".join(
                str(x)
                for x in [addr.get("Name") or addr.get("name"), barrier.get("Name") or barrier.get("name")]
                if x
            )
            for cam in barrier.get("Cameras") or barrier.get("cameras") or []:
                guid = cam.get("Guid") or cam.get("guid") or cam.get("Id") or cam.get("id")
                if not (cam.get("Host") or cam.get("host")):
                    continue
                rows.append(
                    {
                        "name": bar_name,
                        "host": cam.get("Host") or cam.get("host"),
                        "port": cam.get("Port") or cam.get("port"),
                        "guid": guid,
                        "login": cam.get("Login") or cam.get("login"),
                        "password": decrypt_camera_password(
                            cam.get("Password") or cam.get("password")
                        ),
                    }
                )

    if not rows:
        print("Камеры не найдены.", file=sys.stderr)
        return 1

    print("\n# --- вставьте в bridge/config.yaml (секция sources) ---")
    for row in rows:
        print("  - id: egds_camera")  # пользователь переименует
        print(f"    type: egds")
        print(f"    host: {row['host']}")
        print(f"    port: {row['port']}")
        print(f"    guid: {row['guid']}")
        print(f"    login: \"{row['login']}\"")
        print(f"    password: \"{row['password']}\"")
        print(f"    framerate: 2")
        print(f"    # шлагбаум: {row['name']}")
        print()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
