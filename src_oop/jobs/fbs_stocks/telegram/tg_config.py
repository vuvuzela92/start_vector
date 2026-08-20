from __future__ import annotations

from pathlib import Path

TELEGRAM_NOTIFICATIONS_ENABLED = True
TELEGRAM_BOT_TOKEN_ENV = "WB_FBS_TG_BOT_TOKEN"

# chat_id не считается секретом и заполняется здесь явным списком получателей.
TELEGRAM_CHAT_IDS: tuple[str, ...] = ("388986455",)

TELEGRAM_REQUEST_TIMEOUT_SECONDS = 15
TELEGRAM_DEDUP_MINUTES = 60
TELEGRAM_WILD_SUMMARY_THRESHOLD = 3
TELEGRAM_MAX_ARTICLE_EXAMPLES = 8

TELEGRAM_DEDUP_CACHE_PATH = (
    Path(__file__).resolve().parents[1] / "files" / "telegram_notification_cache.json"
)
