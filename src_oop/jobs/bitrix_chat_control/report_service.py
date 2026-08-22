"""Формирование компактных Telegram-саммари по рабочим чатам Bitrix24."""

from __future__ import annotations

from datetime import datetime

from src_oop.jobs.bitrix_chat_control.models import ChatSummary, ChatSummarySection, Problem, ProblemStatus


class BitrixChatReportService:
    """Строит compact daily/weekly/manual summary из состояния PostgreSQL.

    Сервис работает только по данным БД, а не по сырой переписке. Это
    обслуживает главное бизнес-правило прозрачности: итоговый Telegram-отчёт
    должен отражать уже reconciled-состояние проблем, а не заново «гадать» по
    сообщениям каждый раз.
    """

    def build_chat_summary(
        self,
        *,
        chat_name: str,
        period_start: datetime,
        period_end: datetime,
        problems: list[Problem],
    ) -> ChatSummary:
        """Формирует недельное или ручное саммари одного чата в компактном виде.

        Бизнес-правила из ТЗ соблюдаются здесь напрямую: проблемы идут в
        хронологическом порядке, без лишних технических полей, а блок
        «Не решено» показывает только реально открытые или не подтверждённо
        закрытые кейсы.
        """
        period_label = f"{period_start:%d.%m}.{period_start.year}–{period_end:%d.%m.%Y}"
        opened_in_period = [
            problem
            for problem in problems
            if period_start <= problem.first_seen_at.astimezone(period_start.tzinfo) <= period_end
        ]
        resolved_in_period = [
            problem
            for problem in problems
            if problem.resolved_at
            and period_start <= problem.resolved_at.astimezone(period_start.tzinfo) <= period_end
        ]
        unresolved = [
            problem
            for problem in problems
            if problem.status == ProblemStatus.OPEN.value
        ]

        problems_section = ChatSummarySection(
            title="Проблемы",
            lines=[
                f"{problem.first_seen_at.astimezone(period_start.tzinfo):%d.%m}: {problem.title}"
                for problem in sorted(opened_in_period, key=lambda item: item.first_seen_at)
            ],
        )
        resolved_section = ChatSummarySection(
            title="Решено",
            lines=[
                f"{problem.resolved_at.astimezone(period_start.tzinfo):%d.%m}: {problem.title}"
                for problem in sorted(
                    resolved_in_period,
                    key=lambda item: item.resolved_at or item.last_seen_at,
                )
            ],
        )
        unresolved_section = ChatSummarySection(
            title="Не решено",
            lines=[
                f"- {problem.title} — {self._unresolved_reason(problem)}"
                for problem in sorted(unresolved, key=lambda item: item.first_seen_at)
            ],
        )

        status_emoji = self._status_emoji(unresolved)
        conclusion = self._build_conclusion(opened_in_period, unresolved)
        return ChatSummary(
            status_emoji=status_emoji,
            chat_name=chat_name,
            period_label=period_label,
            problems=problems_section,
            resolved=resolved_section,
            unresolved=unresolved_section,
            conclusion=conclusion,
            requires_attention=bool(unresolved),
        )

    def render_summary_text(self, summary: ChatSummary) -> str:
        """Преобразует ChatSummary в Telegram-friendly текст без markdown-магии.

        Финальный текст должен быть коротким и читаемым в обычном чате Telegram,
        поэтому метод выводит только те разделы, которые реально наполнены
        данными, и сохраняет формат `DD.MM: проблема` из ТЗ.
        """
        parts = [f"{summary.status_emoji} {summary.chat_name}", summary.period_label]
        parts.extend(self._render_section(summary.problems))
        parts.extend(self._render_section(summary.resolved))
        parts.extend(self._render_section(summary.unresolved))
        parts.append("Итог:")
        parts.append(summary.conclusion)
        parts.append("")
        parts.append(
            "Требует внимания руководителя: "
            f"{'да' if summary.requires_attention else 'нет'}"
        )
        return "\n".join(parts).strip()

    def render_open_problems_text(self, *, chat_name: str, problems: list[Problem]) -> str:
        """Строит компактный список только открытых проблем для команды `/open`.

        Этот сценарий нужен руководителю для быстрого просмотра текущего хвоста
        незакрытых вопросов без чтения всей хронологии недели.
        """
        lines = [f"Открытые проблемы: {chat_name}"]
        open_problems = [problem for problem in problems if problem.status == ProblemStatus.OPEN.value]
        if not open_problems:
            lines.append("Существенных открытых проблем нет.")
            return "\n".join(lines)
        for problem in sorted(open_problems, key=lambda item: item.first_seen_at):
            lines.append(
                f"- {problem.first_seen_at:%d.%m}: {problem.title} — {self._unresolved_reason(problem)}"
            )
        return "\n".join(lines)

    def render_problem_history_text(self, *, chat_name: str, problems: list[Problem]) -> str:
        """Строит хронологию проблем для команды `/problems`.

        Бизнес-правило: этот экран показывает только историю проблем, в
        хронологическом порядке, без сортировки по критичности или владельцам.
        """
        lines = [f"Хронология проблем: {chat_name}"]
        if not problems:
            lines.append("За выбранный период проблемы не найдены.")
            return "\n".join(lines)
        for problem in sorted(problems, key=lambda item: item.first_seen_at):
            lines.append(f"{problem.first_seen_at:%d.%m}: {problem.title}")
        return "\n".join(lines)

    @staticmethod
    def _render_section(section: ChatSummarySection) -> list[str]:
        """Печатает только непустые секции Telegram-саммари.

        Это сохраняет компактность отчёта: пустые блоки не засоряют сообщение,
        но при наличии данных заголовки секций всегда остаются явными.
        """
        if not section.lines:
            return []
        return ["", f"{section.title}:", *section.lines]

    @staticmethod
    def _unresolved_reason(problem: Problem) -> str:
        """Формирует короткую причину, почему проблема остаётся открытой.

        Бизнес-правило: если явного подтверждения исправления нет, руководитель
        должен это видеть в саммари прямо текстом, а не выводить по умолчанию.
        """
        if problem.last_state_summary:
            return "подтверждения исправления нет"
        return "проблема остаётся открытой"

    @staticmethod
    def _status_emoji(unresolved: list[Problem]) -> str:
        """Определяет зелёный, жёлтый или красный статус чата по хвосту проблем.

        Статус нужен для управленческого обзора недели: красный ставится только
        при блокировках ключевых процессов, значимых рисках или множестве
        нерешённых проблем.
        """
        if not unresolved:
            return "🟢"
        critical_keywords = (
            "блок",
            "склад",
            "штраф",
            "финанс",
            "ошиб",
            "возврат",
            "1с",
        )
        if len(unresolved) >= 3 or any(
            any(keyword in problem.title.lower() for keyword in critical_keywords)
            for problem in unresolved
        ):
            return "🔴"
        return "🟡"

    @staticmethod
    def _build_conclusion(opened_in_period: list[Problem], unresolved: list[Problem]) -> str:
        """Строит короткий итог по чату на человеческом управленческом языке.

        Итог нужен для руководителя, который не хочет читать весь список. Он
        должен быстро понять, были ли системные проблемы и остались ли они
        незакрытыми к концу периода.
        """
        if not opened_in_period and not unresolved:
            return "Существенных проблем за период не выявлено."
        if unresolved:
            return (
                "В чате остаются незакрытые проблемы; требуется контроль выполнения "
                "и подтверждение фактического устранения."
            )
        return "Проблемы периода были зафиксированы и получили подтверждение решения."
