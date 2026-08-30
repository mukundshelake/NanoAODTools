#!/usr/bin/env python3
import os
import sys
import requests
from dotenv import load_dotenv

def send_message(bot_token: str, chat_id: str, text: str):
    """Send a text message via the Telegram Bot API."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()

def main():
    load_dotenv()
    # Expect these to be set in the environment
    bot_token = os.getenv("BOT_TOKEN")
    chat_id   = os.getenv("CHAT_ID")
    if not bot_token or not chat_id:
        print("Error: BOT_TOKEN and CHAT_ID must be set", file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Usage: send_telegram.py \"Your message here\"", file=sys.stderr)
        sys.exit(1)

    message = sys.argv[1]
    try:
        send_message(bot_token, chat_id, message)
    except Exception as e:
        print(f"Failed to send Telegram message: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
