"""Аналитический pipeline для управленческого контроля рабочих чатов Bitrix24."""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from typing import Iterable

from src_oop.jobs.bitrix_chat_control.llm_client import BitrixChatLLMClient, ChatMessageForLLM
from src_oop.jobs.bitrix_chat_control.models import (
    AnalysisCounters,
    BitrixMessage,
    DetectedProblem,
    DetectedResolution,
    ExtractionResult,
    Problem,
    ProblemMessageRelationType,
    ProblemStatus,
    ReconciliationOutcome,
)
from src_oop.jobs.bitrix_chat_control.repository import BitrixChatControlRepository

logger = logging.getLogger(__name__)

_STOP_WORDS = {
    "и",
    "в",
    "во",
    "на",
    "по",
    "с",
    "со",
    "что",
    "это",
    "как",
    "не",
    "но",
    "мы",
    "они",
    "для",
    "при",
    "или",
    "уже",
    "ещё",
    "теперь",
    "там",
    "тут",
    "надо",
    "было",
    "будет",
    "есть",
}
_PROBLEM_HINTS = (
    "ошиб",
    "сбой",
    "не работает",
    "не откры",
    "не подтян",
    "не переда",
    "не можем",
    "невозможно",
    "блокир",
    "завис",
    "ручн",
    "расхож",
    "проблем",
    "возврат",
    "логист",
    "склад",
    "штраф",
    "финанс",
    "интеграц",
    "кредо",
    "киз",
)
_PROBLEM_CONTEXT_HINTS = (
    "разбира",
    "продолжа",
    "под риском",
    "больше ",
)
_PROBLEM_EXCLUDE_HINTS = (
    "обычное уведомление",
    "информационное сообщение",
    "доброе утро",
    "спасибо",
)
_RESOLUTION_HINTS = (
    "исправил",
    "исправили",
    "готово",
    "проверили",
    "теперь работает",
    "работает",
    "списал",
    "нашёл, убрал",
    "нашел, убрал",
    "теперь корректно",
    "можно открывать продажи",
    "решено",
    "закрыли",
)
_RESOLUTION_NEGATIVE_HINTS = (
    "посмотрю",
    "проверю",
    "сделаем",
    "поправим",
    "занимаюсь",
    "передал разработчику",
    "создал задачу",
)
_ENTITY_PATTERN = re.compile(r"\b[^\W_]*[A-Za-zА-Яа-яЁё]+\d+[^\W_]*\b|\b[A-ZА-ЯЁ]{2,}[A-ZА-ЯЁ0-9_/-]*\b")
_TOKEN_PATTERN = re.compile(r"[A-Za-zА-Яа-яЁё0-9_/-]+")
_USER_TAG_PATTERN = re.compile(r"\[USER=\d+(?:\s+REPLACE)?\](.*?)\[/USER\]")
_BRACKET_TAG_PATTERN = re.compile(r"\[(?:USER|CHAT|CALL|DISK FILE|Файл|FILE)[^\]]*\]")
_MULTISPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class _ProblemCandidate:
    """Хранит промежуточный сигнал проблемы до преобразования в Pydantic-модель.

    Внутренняя модель нужна для extraction-эвристики: сначала мы собираем
    кандидаты по сообщениям, затем агрегируем и валидируем их как
    `DetectedProblem`/`DetectedResolution`.
    """

    title: str
    normalized_title: str
    message_id: int
    happened_at: datetime
    summary: str
    entity_tokens: tuple[str, ...]


class BitrixChatAnalysisService:
    """Выделяет проблемы, сопоставляет их с историей и обновляет состояние чата.

    Сервис реализует контролируемый pipeline из extraction и reconciliation без
    «одного огромного промпта». Даже если later подключится LLM, история
    состояний проблемы останется в БД и не будет пересоздаваться заново на
    каждом запуске.
    """

    def __init__(
        self,
        repository: BitrixChatControlRepository | None = None,
        llm_client: BitrixChatLLMClient | None = None,
    ) -> None:
        """Подключает репозиторий состояния проблем и optional LLM extraction.

        Бизнес-смысл зависимости `llm_client` в том, чтобы production-контур мог
        использовать structured output, а локальный и аварийный режимы
        продолжали работать на эвристиках без падения всей синхронизации.
        """
        self.repository = repository or BitrixChatControlRepository()
        self.llm_client = llm_client or BitrixChatLLMClient()

    def analyze_new_messages(
        self,
        *,
        chat_id: int,
        new_messages: list[BitrixMessage],
    ) -> ReconciliationOutcome:
        """Обрабатывает только новые сообщения и обновляет состояние проблем чата.

        Бизнес-правило: повторный запуск sync не должен заново открывать старые
        проблемы. Поэтому метод принимает именно новые, только что сохранённые
        сообщения, а reconciliation делает по уже существующим open-проблемам.
        """
        outcome = ReconciliationOutcome()
        outcome.counters.messages_scanned = len(new_messages)
        outcome.counters.new_messages_count = len(new_messages)
        if not new_messages:
            return outcome

        extraction_result = self.extract(new_messages)
        outcome.extraction_result = extraction_result
        open_problems = self.repository.list_open_problems(chat_id)

        for detected_problem in extraction_result.problems:
            matched_problem = self._match_open_problem(
                detected_problem=detected_problem,
                open_problems=open_problems,
            )
            if matched_problem is None:
                created_problem = self.repository.create_problem(
                    chat_id=chat_id,
                    title=detected_problem.title,
                    normalized_title=detected_problem.normalized_title,
                    first_seen_at=detected_problem.first_seen_at,
                    last_seen_at=detected_problem.first_seen_at,
                    last_state_summary=detected_problem.summary,
                )
                self.repository.link_problem_messages(
                    problem_id=created_problem.id,
                    message_ids=detected_problem.message_ids,
                    relation_type=ProblemMessageRelationType.PROBLEM_SIGNAL,
                )
                open_problems.append(created_problem)
                outcome.counters.new_problems_count += 1
                continue

            self.repository.update_problem_state(
                matched_problem.id,
                last_seen_at=detected_problem.first_seen_at,
                last_state_summary=detected_problem.summary,
                title=self._pick_better_title(matched_problem.title, detected_problem.title),
                normalized_title=detected_problem.normalized_title,
            )
            self.repository.link_problem_messages(
                problem_id=matched_problem.id,
                message_ids=detected_problem.message_ids,
                relation_type=ProblemMessageRelationType.PROBLEM_SIGNAL,
            )
            outcome.counters.updated_problems_count += 1

        for detected_resolution in extraction_result.resolutions:
            matched_problem = self._match_resolution(
                detected_resolution=detected_resolution,
                open_problems=open_problems,
            )
            if matched_problem is None:
                continue
            self.repository.resolve_problem(
                matched_problem.id,
                resolved_at=detected_resolution.resolved_at,
                resolution_summary=detected_resolution.summary,
            )
            self.repository.link_problem_messages(
                problem_id=matched_problem.id,
                message_ids=detected_resolution.message_ids,
                relation_type=ProblemMessageRelationType.RESOLUTION_SIGNAL,
            )
            outcome.counters.resolved_problems_count += 1
            open_problems = [problem for problem in open_problems if problem.id != matched_problem.id]

        return outcome

    def extract(self, messages: list[BitrixMessage]) -> ExtractionResult:
        """Выделяет из новых сообщений структурированные проблемы и решения.

        В production-режиме сервис сначала пытается использовать OpenAI
        structured output. Если пакет, ключ или внешний вызов недоступны,
        extraction деградирует к локальным эвристикам, чтобы не останавливать
        синхронизацию рабочих чатов.
        """
        if self.llm_client.is_available():
            try:
                return self.llm_client.extract(
                    [
                        ChatMessageForLLM(
                            id=message.id,
                            dt=message.message_datetime.isoformat(),
                            author=message.author_name,
                            text=message.message_text,
                        )
                        for message in messages
                    ]
                )
            except Exception as error:
                logger.warning(
                    "LLM extraction недоступен, включён эвристический fallback | error_type=%s",
                    type(error).__name__,
                )

        problem_candidates: list[_ProblemCandidate] = []
        resolution_candidates: list[_ProblemCandidate] = []

        for message in messages:
            cleaned_text = self._clean_message_text(message.message_text)
            if not cleaned_text:
                continue
            if self._is_ignorable_message(message, cleaned_text):
                continue
            if self._looks_like_resolution(cleaned_text):
                resolution_candidates.append(self._build_candidate(message, cleaned_text))
                continue
            if self._looks_like_problem(cleaned_text):
                problem_candidates.append(self._build_candidate(message, cleaned_text))

        return ExtractionResult(
            problems=self._collapse_problem_candidates(problem_candidates),
            resolutions=self._collapse_resolution_candidates(resolution_candidates),
        )

    def _collapse_problem_candidates(
        self,
        candidates: list[_ProblemCandidate],
    ) -> list[DetectedProblem]:
        """Склеивает повторяющиеся сигналы одной проблемы в один extraction-объект.

        Это обслуживает правило дедупликации: несколько сообщений про тот же
        сбой не должны попадать в отчёт как разные независимые проблемы.
        """
        grouped: dict[str, list[_ProblemCandidate]] = {}
        for candidate in candidates:
            group_key = self._group_key(candidate.normalized_title, candidate.entity_tokens)
            grouped.setdefault(group_key, []).append(candidate)

        result: list[DetectedProblem] = []
        for group in grouped.values():
            group.sort(key=lambda item: item.happened_at)
            first = group[0]
            if not self._is_valid_candidate_title(first.title, first.normalized_title):
                continue
            result.append(
                DetectedProblem(
                    title=first.title,
                    normalized_title=first.normalized_title,
                    message_ids=[item.message_id for item in group],
                    first_seen_at=first.happened_at,
                    summary=group[-1].summary,
                    entity_tokens=list(first.entity_tokens),
                )
            )
        return result

    def _collapse_resolution_candidates(
        self,
        candidates: list[_ProblemCandidate],
    ) -> list[DetectedResolution]:
        """Склеивает повторяющиеся сигналы решения одной и той же проблемы.

        Это защищает reconciliation от повторных одинаковых закрытий, если
        участники чата несколько раз подтверждали одно и то же исправление.
        """
        grouped: dict[str, list[_ProblemCandidate]] = {}
        for candidate in candidates:
            group_key = self._group_key(candidate.normalized_title, candidate.entity_tokens)
            grouped.setdefault(group_key, []).append(candidate)

        result: list[DetectedResolution] = []
        for group in grouped.values():
            group.sort(key=lambda item: item.happened_at)
            last = group[-1]
            if not self._is_valid_candidate_title(last.title, last.normalized_title):
                continue
            result.append(
                DetectedResolution(
                    title=last.title,
                    normalized_title=last.normalized_title,
                    message_ids=[item.message_id for item in group],
                    resolved_at=last.happened_at,
                    summary=last.summary,
                    entity_tokens=list(last.entity_tokens),
                )
            )
        return result

    def _build_candidate(
        self,
        message: BitrixMessage,
        cleaned_text: str,
    ) -> _ProblemCandidate:
        """Преобразует сообщение в нормализованный problem/resolution candidate.

        Вспомогательный шаг защищает основной pipeline от разрозненной логики
        очистки текста и выделения сущностей, чтобы и extraction, и matching
        использовали одинаковые правила нормализации.
        """
        title = self._short_title(cleaned_text)
        normalized_title = self.normalize_title(title)
        entity_tokens = tuple(self.extract_entity_tokens(cleaned_text))
        return _ProblemCandidate(
            title=title,
            normalized_title=normalized_title,
            message_id=message.id,
            happened_at=message.message_datetime.astimezone(UTC),
            summary=cleaned_text,
            entity_tokens=entity_tokens,
        )

    def _match_open_problem(
        self,
        *,
        detected_problem: DetectedProblem,
        open_problems: Iterable[Problem],
    ) -> Problem | None:
        """Ищет, относится ли новый сигнал к уже открытой проблеме.

        Это обслуживает главное бизнес-правило истории состояния: один и тот же
        кейс должен обновлять существующую проблему, а не плодить дубликаты.
        """
        best_problem: Problem | None = None
        best_score = 0.0
        for open_problem in open_problems:
            score = self._problem_similarity(
                normalized_title_a=detected_problem.normalized_title,
                normalized_title_b=open_problem.normalized_title,
                entity_tokens_a=detected_problem.entity_tokens,
                entity_tokens_b=self.extract_entity_tokens(open_problem.title),
            )
            if score > best_score:
                best_problem = open_problem
                best_score = score
        return best_problem if best_score >= 0.55 else None

    def _match_resolution(
        self,
        *,
        detected_resolution: DetectedResolution,
        open_problems: list[Problem],
    ) -> Problem | None:
        """Подбирает открытую проблему, которую можно перевести в `resolved`.

        Бизнес-правило: generic-фраза «проверили, теперь работает» может закрыть
        только одну очевидную открытую проблему. Если открытых проблем несколько и
        resolution не даёт контекста, лучше оставить состояние `open`.
        """
        if not open_problems:
            return None
        if len(open_problems) == 1 and not detected_resolution.entity_tokens:
            return open_problems[0]

        best_problem: Problem | None = None
        best_score = 0.0
        for open_problem in open_problems:
            score = self._problem_similarity(
                normalized_title_a=detected_resolution.normalized_title,
                normalized_title_b=open_problem.normalized_title,
                entity_tokens_a=detected_resolution.entity_tokens,
                entity_tokens_b=self.extract_entity_tokens(open_problem.title),
            )
            if score > best_score:
                best_problem = open_problem
                best_score = score
        return best_problem if best_score >= 0.55 else None

    @staticmethod
    def _pick_better_title(current_title: str, new_title: str) -> str:
        """Выбирает более полезный для Telegram заголовок проблемы.

        Если новый сигнал содержит более длинное и предметное описание, лучше
        обновить title, чтобы weekly-отчёт показывал человеку понятную формулировку.
        """
        return new_title if len(new_title) > len(current_title) else current_title

    @staticmethod
    def normalize_title(text: str) -> str:
        """Нормализует заголовок проблемы для дедупликации и reconciliation.

        Нормализация защищает приложение от дублей, вызванных регистром,
        пунктуацией и служебными словами, но оставляет доменные токены вроде
        `ФБС_1`, `1С`, `wild2042`, которые важны для бизнес-смысла.
        """
        tokens = []
        for token in _TOKEN_PATTERN.findall(text.lower()):
            if token in _STOP_WORDS or len(token) < 2:
                continue
            tokens.append(token)
        return " ".join(tokens)

    @staticmethod
    def extract_entity_tokens(text: str) -> list[str]:
        """Извлекает доменные идентификаторы и сущности для смыслового матчинга.

        Это защищает дедупликацию от ложного размножения проблем, когда разные
        сообщения используют разную формулировку, но говорят про один и тот же
        объект вроде `ФБС_1`, `1С`, `wild2042` или конкретный контур.
        """
        return [token.upper() for token in _ENTITY_PATTERN.findall(text)]

    @staticmethod
    def _short_title(text: str) -> str:
        """Укорачивает сообщение до компактного заголовка проблемы/решения.

        Бизнес-сценарий: Telegram-саммари должно быть компактным. Поэтому заголовок
        берётся из первой смысловой части сообщения и обрезается до безопасной длины.
        """
        title = re.split(r"[.!?]", text, maxsplit=1)[0].strip()
        if len(title) > 180:
            return title[:177].rstrip() + "..."
        return title

    @staticmethod
    def _is_valid_candidate_title(title: str, normalized_title: str) -> bool:
        """Отбрасывает шумные короткие заголовки до валидации Pydantic-модели.

        Это защищает production-sync от падений на служебных числах, одиночных
        кодах и случайных коротких фрагментах, которые не несут полноценного
        управленческого смысла для блока проблем или решений.
        """
        prepared_title = title.strip()
        prepared_normalized = normalized_title.strip()
        if len(prepared_title) < 3 or len(prepared_normalized) < 2:
            return False
        if prepared_title.isdigit():
            return False
        return True

    @staticmethod
    def _clean_message_text(text: str) -> str:
        """Очищает Bitrix-разметку и шум перед анализом смысла сообщения.

        В рабочих чатах Bitrix текст часто содержит служебные теги `USER`,
        упоминания, маркеры файлов и лишние переводы строк. Для quality
        extraction важно сначала превратить это в нормальный человекочитаемый
        текст, иначе и LLM, и эвристики ловят лишний шум вместо проблемы.
        """
        prepared = _USER_TAG_PATTERN.sub(lambda match: match.group(1).strip(), text)
        prepared = _BRACKET_TAG_PATTERN.sub(" ", prepared)
        prepared = prepared.replace("\t", " ").replace("\r", " ").replace("\n", " ")
        prepared = _MULTISPACE_PATTERN.sub(" ", prepared).strip()
        return prepared

    @staticmethod
    def _is_ignorable_message(message: BitrixMessage, cleaned_text: str) -> bool:
        """Отсекает системные и заведомо нешумовые сообщения до extraction.

        Это защищает управленческое саммари от мусора вроде приглашений в чат,
        служебных join-событий и пустых технических уведомлений, которые не
        несут бизнес-смысла для блока проблем.
        """
        if not cleaned_text:
            return True
        raw_payload = message.raw_payload_json or {}
        params = raw_payload.get("params")
        if isinstance(params, dict):
            codes = params.get("CODE")
            if isinstance(codes, list) and any(code in {"CHAT_JOIN", "USER_INVITE"} for code in codes):
                return True
        normalized = cleaned_text.lower()
        if "пригласил в чат" in normalized:
            return True
        return False

    @staticmethod
    def _looks_like_problem(text: str) -> bool:
        """Эвристически определяет, несёт ли сообщение сигнал реальной проблемы.

        Это обслуживает базовое бизнес-правило MVP: не считать проблемами обычные
        вопросы, информационные сообщения и нейтральное обсуждение без явной боли.
        """
        normalized = text.lower()
        if any(exclude in normalized for exclude in _PROBLEM_EXCLUDE_HINTS):
            return False
        if normalized.endswith("?") and len(normalized) < 40 and "проблем" not in normalized:
            return False
        if normalized in {"поправлю", "готово", "спасибо", "поняла, спасибо"}:
            return False
        if any(hint in normalized for hint in _PROBLEM_HINTS):
            return True
        entity_tokens = BitrixChatAnalysisService.extract_entity_tokens(text)
        if entity_tokens and any(hint in normalized for hint in _PROBLEM_CONTEXT_HINTS):
            return True
        return False

    @staticmethod
    def _looks_like_resolution(text: str) -> bool:
        """Определяет, содержит ли сообщение явное подтверждение решения проблемы.

        Метод специально защищает контур от ложных закрытий: обещания действий,
        проверки и постановки задач не переводят проблему в `resolved`.
        """
        normalized = text.lower()
        if any(negative in normalized for negative in _RESOLUTION_NEGATIVE_HINTS):
            return False
        return any(positive in normalized for positive in _RESOLUTION_HINTS)

    @staticmethod
    def _group_key(normalized_title: str, entity_tokens: Iterable[str]) -> str:
        """Строит ключ группировки повторных сигналов одной проблемы.

        Если в сообщении есть доменная сущность, она важнее общих слов. Это
        помогает склеивать сообщения вроде «ФБС_1 блокирует склад» и «по ФБС_1
        продолжаем разбираться», даже если текстовое описание отличается.
        """
        entities = list(dict.fromkeys(entity_tokens))
        if entities:
            return entities[0]
        return normalized_title

    @staticmethod
    def _problem_similarity(
        *,
        normalized_title_a: str,
        normalized_title_b: str,
        entity_tokens_a: Iterable[str],
        entity_tokens_b: Iterable[str],
    ) -> float:
        """Оценивает смысловую близость двух проблем без тяжёлой ML-модели.

        Формула нужна для reconciliation в MVP: она комбинирует совпадение
        доменных сущностей и текстовую близость, чтобы бережно обновлять историю
        одной и той же проблемы без агрессивного слияния разных кейсов.
        """
        set_a = set(entity_tokens_a)
        set_b = set(entity_tokens_b)
        if set_a and set_b and set_a == set_b:
            return 0.95
        if set_a and set_b and set_a.intersection(set_b):
            return 0.75

        if normalized_title_a == normalized_title_b:
            return 0.9

        tokens_a = set(normalized_title_a.split())
        tokens_b = set(normalized_title_b.split())
        if tokens_a and tokens_b:
            jaccard = len(tokens_a.intersection(tokens_b)) / len(tokens_a.union(tokens_b))
        else:
            jaccard = 0.0
        ratio = SequenceMatcher(None, normalized_title_a, normalized_title_b).ratio()
        return max(jaccard, ratio)
