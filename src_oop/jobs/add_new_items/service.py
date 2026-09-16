from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Sequence

from src_oop.jobs.add_new_items.config import (
    AUTOPILOT_INSERT_INDEXES,
    AUTOPILOT_SKU_HEADER,
    SHEETS,
    SOPOST_ADD_FLAG,
    SOPOST_INSERT_HEADERS,
    STATUS_NO,
    STATUS_YES,
    UNIT_MAIN_COMMISSION_VALUE,
    UNIT_MAIN_INSERT_HEADERS,
    UNIT_MAIN_SKU_HEADER,
)
from src_oop.jobs.add_new_items.models import NewItemCard
from src_oop.jobs.add_new_items.repository import AddNewItemsRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AddNewItemsResult:
    """Сводка по выполнению job add_new_items."""

    loaded_cards: int
    added_to_sopost: int
    added_to_unit_main: int
    added_to_autopilot: int
    added_to_competitors: int
    added_to_products: int


@dataclass(slots=True)
class AddNewItemsService:
    """Оркеструет перенос новых товаров по рабочим таблицам."""

    repository: AddNewItemsRepository

    def run(self) -> AddNewItemsResult:
        """Запускает полный сценарий переноса и финальный контроль дублей.

        Job берёт строки со статусом `добавить` из вкладки «Для юнит», переносит
        недостающие данные в Сопост, MAIN (tested), Автопилот, Конкуренты и
        products, обновляет флаги результата в источнике, а затем проверяет
        целевые таблицы MAIN (tested) и Автопилот на дубли SKU для отчёта
        сотрудникам в Telegram.
        """
        cards = self.repository.fetch_new_item_cards()
        if not cards:
            logger.info("Нет строк со статусом 'добавить'. Завершаем job.")
            return AddNewItemsResult(0, 0, 0, 0, 0, 0)

        status_by_row = {
            card.row_number: {
                "unit_main": STATUS_NO,
                "autopilot": STATUS_NO,
                "products": STATUS_NO,
            }
            for card in cards
        }

        unique_wild_cards = self._unique_by(cards, key=lambda card: card.wild)
        unique_sku_cards = self._unique_by(cards, key=lambda card: card.sku)

        existing_sopost_wilds = self.repository.fetch_existing_wilds_in_sopost()
        existing_unit_main_skus = self.repository.fetch_existing_skus_in_unit_main()
        existing_autopilot_skus = self.repository.fetch_existing_skus_in_autopilot()
        existing_competitors_wilds = self.repository.fetch_existing_wilds_in_competitors()
        existing_product_wilds = self.repository.fetch_existing_product_wilds()

        sopost_cards = [
            card for card in unique_wild_cards if card.wild not in existing_sopost_wilds
        ]
        unit_main_cards = [
            card for card in unique_sku_cards if card.sku not in existing_unit_main_skus
        ]
        autopilot_cards = [
            card for card in unique_sku_cards if card.sku not in existing_autopilot_skus
        ]
        competitors_cards = [
            card for card in unique_wild_cards if card.wild not in existing_competitors_wilds
        ]
        products_cards = [
            card for card in unique_wild_cards if card.wild not in existing_product_wilds
        ]

        added_to_sopost = self._append_sopost_rows(sopost_cards)
        added_to_unit_main = self._append_unit_main_rows(unit_main_cards)
        added_to_autopilot = self._append_autopilot_rows(autopilot_cards)
        added_to_competitors = self._append_competitors_rows(competitors_cards)
        added_to_products, successful_product_wilds = self._append_products(products_cards)

        existing_unit_main_sku_set = existing_unit_main_skus | {card.sku for card in unit_main_cards}
        existing_autopilot_sku_set = existing_autopilot_skus | {card.sku for card in autopilot_cards}
        existing_products_wild_set = existing_product_wilds | successful_product_wilds

        for card in cards:
            status_by_row[card.row_number]["unit_main"] = (
                STATUS_YES if card.sku in existing_unit_main_sku_set else STATUS_NO
            )
            status_by_row[card.row_number]["autopilot"] = (
                STATUS_YES if card.sku in existing_autopilot_sku_set else STATUS_NO
            )
            status_by_row[card.row_number]["products"] = (
                STATUS_YES if card.wild in existing_products_wild_set else STATUS_NO
            )

        self.repository.update_input_status_flags(status_by_row=status_by_row)
        self._log_target_duplicate_summary()

        result = AddNewItemsResult(
            loaded_cards=len(cards),
            added_to_sopost=added_to_sopost,
            added_to_unit_main=added_to_unit_main,
            added_to_autopilot=added_to_autopilot,
            added_to_competitors=added_to_competitors,
            added_to_products=added_to_products,
        )
        logger.info("Job add_new_items завершён: %s", result)
        return result

    def _append_sopost_rows(self, cards: Sequence[NewItemCard]) -> int:
        rows = [
            [
                card.category,
                card.item_name,
                card.wild,
                card.supplier_code_duplicates or card.wild,
                card.normalized_purchase_price,
                SOPOST_ADD_FLAG,
            ]
            for card in cards
        ]
        return self.repository.append_rows_by_headers(
            SHEETS.sopost,
            rows,
            SOPOST_INSERT_HEADERS,
        )

    def _append_unit_main_rows(self, cards: Sequence[NewItemCard]) -> int:
        """Добавляет новые SKU в MAIN (tested) с обязательной комиссией по ИУ.

        Для каждого нового SKU переносит артикул, личный кабинет и wild из
        вкладки «Для юнит». Комиссия по ИУ фиксирована на уровне бизнес-правила
        и должна составлять 26% для каждой строки, созданной этим job.
        """
        rows = [
            [
                card.sku,
                card.client,
                card.wild,
                UNIT_MAIN_COMMISSION_VALUE,
            ]
            for card in cards
        ]
        return self.repository.append_rows_by_headers(
            SHEETS.unit_main,
            rows,
            UNIT_MAIN_INSERT_HEADERS,
        )

    def _append_autopilot_rows(self, cards: Sequence[NewItemCard]) -> int:
        rows = [
            [card.sku, card.category, card.autopilot_client, card.wild]
            for card in cards
        ]
        return self.repository.append_rows_by_indexes(
            SHEETS.autopilot,
            rows,
            AUTOPILOT_INSERT_INDEXES,
        )

    def _append_competitors_rows(self, cards: Sequence[NewItemCard]) -> int:
        rows = [[card.wild] for card in cards]
        return self.repository.append_rows_by_headers(
            SHEETS.competitors,
            rows,
            ("Укажи вилд",),
        )

    def _append_products(self, cards: Sequence[NewItemCard]) -> tuple[int, set[str]]:
        if not cards:
            return 0, set()

        valid_cards: list[NewItemCard] = []
        for card in cards:
            if not self._is_valid_product_wild(card.wild):
                logger.error(
                    "Пропускаем запись в products из-за неверного формата wild: row=%s wild=%s",
                    card.row_number,
                    card.wild,
                )
                continue
            valid_cards.append(card)

        if not valid_cards:
            return 0, set()

        try:
            product_records = asyncio.run(self.repository.build_missing_products(valid_cards))
            inserted_count = self.repository.upsert_products(product_records)
        except Exception:
            logger.exception("Не удалось записать данные в products.")
            return 0, set()

        return inserted_count, {record.wild for record in product_records}

    def _log_target_duplicate_summary(self) -> None:
        """Пишет в лог итоговую проверку дублей в таблицах для Telegram-отчёта.

        После вставки данных сотрудникам важно быстро увидеть, остались ли в
        MAIN (tested) или Автопилоте повторяющиеся SKU. Лог пишется в стабильном
        формате, чтобы Telegram-бот мог распознать его и показать аккуратную
        сводку без технического хвоста выполнения.
        """
        unit_main_duplicates = self.repository.fetch_duplicate_int_values(
            SHEETS.unit_main,
            UNIT_MAIN_SKU_HEADER,
        )
        autopilot_duplicates = self.repository.fetch_duplicate_int_values(
            SHEETS.autopilot,
            AUTOPILOT_SKU_HEADER,
        )
        logger.info(
            "Проверка дублей add_new_items: "
            "MAIN (tested): duplicated_values=%s extra_rows=%s examples=%s | "
            "Автопилот: duplicated_values=%s extra_rows=%s examples=%s",
            len(unit_main_duplicates),
            self._count_extra_duplicate_rows(unit_main_duplicates),
            self._format_duplicate_examples(unit_main_duplicates),
            len(autopilot_duplicates),
            self._count_extra_duplicate_rows(autopilot_duplicates),
            self._format_duplicate_examples(autopilot_duplicates),
        )

    @staticmethod
    def _count_extra_duplicate_rows(duplicates: dict[int, int]) -> int:
        """Считает лишние повторные строки среди найденных дублей SKU.

        Для бизнес-отчёта недостаточно знать количество артикулов с дублями:
        сотруднику полезно видеть примерный объём ручной чистки, поэтому каждая
        первая строка SKU считается допустимой, а остальные входят в показатель
        лишних повторов.
        """
        return sum(count - 1 for count in duplicates.values())

    @staticmethod
    def _format_duplicate_examples(duplicates: dict[int, int]) -> str:
        """Формирует короткие примеры дублей SKU для итогового сообщения.

        Telegram-отчёт должен оставаться лаконичным, поэтому показываются только
        первые несколько SKU в человекочитаемом виде. Полный список дублей
        можно увидеть уже в самой Google-таблице по колонке `Артикул`.
        """
        if not duplicates:
            return "нет"

        examples = [
            f"SKU {sku} встречается {count} {AddNewItemsService._pluralize_times(count)}"
            for sku, count in sorted(duplicates.items())[:5]
        ]
        return "; ".join(examples)

    @staticmethod
    def _pluralize_times(count: int) -> str:
        """Подбирает русскую форму слова `раз` для примеров дублей.

        Отчёт читают сотрудники, а не разработчики, поэтому количество повторов
        SKU показывается обычной фразой: `2 раза`, `5 раз`, без технических
        сокращений вроде `x2`.
        """
        if 11 <= count % 100 <= 14:
            return "раз"
        if count % 10 in {2, 3, 4}:
            return "раза"
        return "раз"

    @staticmethod
    def _unique_by(
        cards: Sequence[NewItemCard],
        *,
        key,
    ) -> list[NewItemCard]:
        seen: set[object] = set()
        result: list[NewItemCard] = []
        for card in cards:
            marker = key(card)
            if marker in seen:
                continue
            seen.add(marker)
            result.append(card)
        return result

    @staticmethod
    def _is_valid_product_wild(wild: str) -> bool:
        normalized = wild.strip().lower()
        return normalized.startswith("wild") and normalized[4:].isdigit()
