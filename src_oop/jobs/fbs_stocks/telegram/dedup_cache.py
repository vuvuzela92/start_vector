from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path


class TelegramNotificationDedupCache:
    """Хранит временные ключи уведомлений, чтобы cron не слал одинаковый Telegram-спам.

    Бизнес-сценарий: FBS-задачи могут запускаться каждые несколько минут. Если одна и та же ошибка
    повторяется на каждом прогоне, оператору достаточно получить одно уведомление за окно времени,
    а не десятки сообщений подряд.
    """

    def __init__(self, cache_path: Path, dedup_minutes: int) -> None:
        """Настраивает файл кэша и TTL подавления повторных уведомлений."""
        self.cache_path = cache_path
        self.dedup_minutes = dedup_minutes

    def should_send(self, key: str, now: datetime) -> bool:
        """Проверяет, можно ли отправить уведомление с данным ключом в текущее время.

        Бизнес-правило: идентичные уведомления в пределах TTL подавляются, но более поздний повтор
        после истечения окна снова допустим, чтобы оператор видел, что проблема не исчезла.
        """
        cache = self._load_cache()
        self._prune(cache=cache, now=now)
        last_sent_raw = cache.get(key)
        if last_sent_raw:
            try:
                last_sent = datetime.fromisoformat(last_sent_raw)
            except ValueError:
                last_sent = None
            if last_sent is not None and now - last_sent < timedelta(minutes=self.dedup_minutes):
                self._save_cache(cache)
                return False
        cache[key] = now.isoformat()
        self._save_cache(cache)
        return True

    def _load_cache(self) -> dict[str, str]:
        """Читает JSON-кэш дедупликации, сохраняя работоспособность при пустом или битом файле."""
        if not self.cache_path.exists():
            return {}
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(data, dict):
            return {}
        return {
            str(key): str(value)
            for key, value in data.items()
            if isinstance(key, str) and isinstance(value, str)
        }

    def _save_cache(self, cache: dict[str, str]) -> None:
        """Сохраняет JSON-кэш дедупликации рядом с FBS-сценариями."""
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _prune(self, cache: dict[str, str], now: datetime) -> None:
        """Удаляет из кэша устаревшие записи, чтобы файл не разрастался бесконечно."""
        cutoff = now - timedelta(minutes=self.dedup_minutes)
        stale_keys: list[str] = []
        for key, raw_value in cache.items():
            try:
                saved_at = datetime.fromisoformat(raw_value)
            except ValueError:
                stale_keys.append(key)
                continue
            if saved_at < cutoff:
                stale_keys.append(key)
        for key in stale_keys:
            cache.pop(key, None)
