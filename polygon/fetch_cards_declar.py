import asyncio
import time
from pathlib import Path
import re


import aiohttp
from loguru import logger
import pandas as pd

from .utils import get_wb_tokens
from polygon.wb_clients.wb_cards import CardsWBAPI, Card


# - 15001138 - Дата окончания действия сертификата/декларации
# - 15001137 - Дата регистрации сертификата/декларации
# - 15001135 - Номер декларации соответствия
# - 15001136 - Номер сертификата соответствия


def extract_prefix(s: str) -> str | None:
    match = re.match(r'^wild\d+', s.lower())
    return match.group(0) if match else None


def fetch_tnvd_by_card(card: Card) -> dict[int, str | None]:
    charcs = card.characteristics

    for ch in charcs:
        if ch.id == 15000001:
            if ch.value:
                logger.info(f"Найден ТНВЭД-код для карточки: {card.nm_id} - {ch.id} - {ch.name} - {ch.value}")
                return next((i for i in ch.value), None)


def prepare_excel_data(results: dict[str, dict[int, dict[int, str | None]]]) -> list[dict]:
    """Подготавливает данные для выгрузки в Excel."""
    rows = []

    for account_name, (cards_data, other_card_data) in results.items():
        for nm_id, charcs in cards_data.items():
            decl_number = charcs.get(15001135)
            cert_number = charcs.get(15001136)
            decl_reg_date = charcs.get(15001137)
            decl_expiry_date = charcs.get(15001138)
            local_vendor_code, subject_name, tnvad = other_card_data.get(nm_id)

            row = {
                "Аккаунт": account_name,
                "nm_id": nm_id,
                "wild": local_vendor_code,
                "Предмет": subject_name,
                "ТНВЭД-код": tnvad,
                "Номер декларации": decl_number,
                "Номер сертификата": cert_number,
                "Дата регистрации сертификата/декларации": decl_reg_date,
                "Дата окончания действия сертификата/декларации": decl_expiry_date,
            }

            has_declaration = bool(decl_number and str(decl_number).strip())
            has_certificate = bool(cert_number and str(cert_number).strip())

            row["Декларация заполнена"] = "Да" if has_declaration else "Нет"
            row["Сертификат заполнен"] = "Да" if has_certificate else "Нет"
            row["Декларация или сертификат заполнены"] = "Да" if (has_declaration or has_certificate) else "Нет"

            rows.append(row)

    return rows


def export_to_excel(data: list[dict], filename: str = "certification_report.xlsx") -> str:
    """Экспортирует данные в Excel файл."""
    if not data:
        logger.warning("Нет данных для выгрузки в Excel")
        return ""

    df = pd.DataFrame(data)

    columns_order = [
        "Аккаунт",
        "nm_id",
        "wild",
        "Предмет",
        "ТНВЭД-код",
        "Декларация или сертификат заполнены",
        "Декларация заполнена",
        "Номер декларации",
        "Сертификат заполнен",
        "Номер сертификата",
        "Дата регистрации сертификата/декларации",
        "Дата окончания действия сертификата/декларации",
    ]

    existing_columns = [col for col in columns_order if col in df.columns]
    other_columns = [col for col in df.columns if col not in columns_order]
    df = df[existing_columns + other_columns]
    output_path = Path(filename)
    df.to_excel(output_path, index=False, engine="openpyxl")
    
    logger.info(f"Данные выгружены в файл: {output_path.absolute()}")
    return str(output_path.absolute())


def fetch_data_declar_and_certification_of_card(card: Card) -> dict[int, dict[int, str | None]]:
    declar_cert_map = {
        15001138: None,
        15001137: None,
        15001135: None,
        15001136: None,
    }

    logger.info(f"Получаем данные о декларации/сертификации для карточки: {card.nm_id}...")
    charcs = card.characteristics

    for ch in charcs:
        if ch.id in declar_cert_map:
            if ch.value:
                declar_cert_map[ch.id] = next((i for i in ch.value), None)

    return {
        card.nm_id: declar_cert_map
    }


async def get_all_data_declaration_and_certification_cards(
        session: aiohttp.ClientSession,
        account_name: str,
) -> dict[str, dict[int, dict[int, str | None]]]:
    logger.info(f"Сбор данных о сертификации/декларации карточек для аккаунта: {account_name}...")
    wb_client = CardsWBAPI(session=session, account_name=account_name)
    all_declar_cert_data = {}
    other_card_data = {}

    async for card in wb_client.iter_exists_cards():
        declar_cert_data = fetch_data_declar_and_certification_of_card(card)
        all_declar_cert_data.update(declar_cert_data)
        other_card_data[card.nm_id] = extract_prefix(card.vendor_code), card.subject_name, fetch_tnvd_by_card(card)
    
    return {
        account_name: (all_declar_cert_data, other_card_data)
    }

async def main():
    tokens = await get_wb_tokens()
    all_results = {}

    try:
        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(get_all_data_declaration_and_certification_cards(
                    account_name=acc,
                    session=session,
                ))
                for acc, _ in tokens.items()
            ]

            results = await asyncio.gather(*tasks)

            for result in results:
                all_results.update(result)

            excel_data = prepare_excel_data(all_results)
            # Поменять на выгрузку в Юнитку
            export_to_excel(excel_data)
    except Exception as e:
        logger.exception(f"Необработанное исключение: {e}")


if __name__ == "__main__":
    logger.info("Старт скрипта...")
    start_time = time.perf_counter()

    try:
        asyncio.run(main())
        logger.info("Завершено!")
    finally:
        end_time = time.perf_counter()
        elapsed = end_time - start_time

        hours, rem = divmod(elapsed, 3600)
        minutes, seconds = divmod(rem, 60)

        if hours:
            time_str = f"{int(hours)} ч {int(minutes)} мин {seconds:.2f} сек"
        elif minutes:
            time_str = f"{int(minutes)} мин {seconds:.2f} сек"
        else:
            time_str = f"{seconds:.2f} сек"

        logger.info(f"Время выполнения: {time_str}")
