import logging
import re
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.core.utils_general import clean_currency_value
from src_oop.jobs.calculation_of_purchases_china.config import (
    LOGISTICS_VED_REQUIRED_COLUMNS,
    ORDERS_WHITE_UPDATED_AT_COLUMN,
    VED_DIGIT_COLS,
    VED_PAYMENT_CONFIGS,
    delivery_calculation_china,
    logistics_ved,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class VedAlignmentResult:
    """
    Хранит результат приведения `ved_balance_df` к структуре `balance_df`.

    Этот объект нужен на промежуточном этапе pipeline, когда данные по ВЭД уже
    рассчитаны по этапам платежей, но еще не готовы к безопасному объединению
    с результатом `OrdersWhiteBalanceAnalyticsService`.

    Почему здесь отдельный dataclass:
    - после выравнивания нам важен не только сам DataFrame;
    - при отладке нужно видеть, каких колонок не хватало в VED-части;
    - также важно понимать, какие колонки в VED есть, но в `balance_df` их нет.

    Атрибуты:
        df_aligned:
            DataFrame, в котором VED-данные уже приведены к порядку и набору
            колонок из `balance_df`.
        missing_columns:
            Колонки, которых не было в VED и которые пришлось добавить
            с пустыми значениями для совместимости.
        extra_columns:
            Колонки, которые были в VED-результате, но отсутствуют в `balance_df`.
            Они полезны для диагностики и проверки, не потеряли ли мы важные данные.
    """

    df_aligned: pd.DataFrame
    missing_columns: list[str]
    extra_columns: list[str]


class VedBalanceAnalyticsService:
    """
    Формирует платежную аналитику по таблице `logistics_ved` и готовит ее
    к объединению с `balance_df`, который уже строится для `orders_white`.

    Бизнес-задача класса:
    - загрузить данные из таблицы "Логистика ВЭД 2026";
    - проверить, что в таблице есть обязательные колонки;
    - привести числовые поля к рабочему виду;
    - развернуть широкую строку поставки в несколько строк платежей по этапам;
    - привести результат к структуре, совместимой с `balance_df`;
    - подготовить объединенный результат к тестовой выгрузке.

    С какими данными работает класс:
    - на входе берет широкую таблицу ВЭД, где в одной строке лежат общие данные
      по поставке и сразу несколько разных видов расходов;
    - на выходе формирует узкий платежный DataFrame, где каждая строка
      соответствует конкретному этапу платежа.

    На каком этапе pipeline используется:
    - после расчета `balance_df` для белых заказов;
    - перед объединением VED-данных с уже существующей аналитикой;
    - перед выгрузкой объединенного результата в тестовый или production-лист.

    Почему нужен отдельно, а не встроен в существующую логику:
    - `VED_PAYMENT_CONFIGS` и `ORDERS_WHITE_PAYMENT_CONFIGS` похожи только внешне,
      но не являются полностью идентичными по структуре и бизнес-смыслу;
    - у VED своя исходная таблица, свой набор колонок и свои этапы платежей;
    - изоляция VED-логики уменьшает риск случайно сломать рабочий pipeline
      `OrdersWhiteBalanceAnalyticsService`.
    """

    BASE_COLUMNS_MAP = {
        "Логист": "Закупщик",
        "№ ПРОФОРМЫ": "№ проформы в документах и 1С",
        "Статус": "Статус",
        "АРТИКУЛЫ": "wild",
        "НАИМЕНОВАНИЕ ТОВАРА": "Модель",
        "Юр лицо": "КОМПАНИЯ",
        "КОЛИЧЕСТВО ШТУК": "Кол-во к заказу",
        "ТРАНСПОРТНАЯ КОМПАНИЯ": "Поставщик",
    }
    
    DEFAULT_COLUMNS = {
        "Номер заказа 1С": pd.NA,
        "ФИН Статус": pd.NA,
        "Дата_аванса_по_годовому_плану": pd.NA,
        "Дата_факт": pd.NA,
        "%_оплаты": pd.NA,
    }

    @staticmethod
    def get_duplicate_risk_stage_numbers() -> list[int]:
        """
        Находит этапы VED, у которых конфиг указывает на одинаковые исходные колонки.

        Что делает метод:
        - проходит по `VED_PAYMENT_CONFIGS`;
        - превращает словарь `columns` каждого этапа в нормализованный набор пар;
        - ищет полностью совпадающие mappings между разными этапами.

        Зачем это нужно:
        если два этапа читают одни и те же исходные колонки, то при развороте
        строк по этапам мы можем получить одинаковые строки платежей.
        Это не обязательно ошибка в коде, но точно повод для бизнес-проверки.

        Особенно важно для этапов 5 и 6:
        - мы не удаляем один из этапов автоматически;
        - не меняем бизнес-логику без подтверждения;
        - лишь честно предупреждаем, что одинаковые источники данных
          могут привести к дублям и требуют проверки.

        Возвращает:
            Список номеров этапов, которые используют совпадающие mappings.
            Если совпадений нет, вернется пустой список.
        """
        seen_column_maps: dict[tuple[tuple[str, str], ...], int] = {}
        duplicate_stage_numbers: list[int] = []

        for payment_config in VED_PAYMENT_CONFIGS:
            stage_number = int(payment_config["Номер этапа платежа"])
            column_map = tuple(sorted(payment_config["columns"].items()))
            if column_map in seen_column_maps:
                duplicate_stage_numbers.append(stage_number)
                duplicate_stage_numbers.append(seen_column_maps[column_map])
                continue

            seen_column_maps[column_map] = stage_number

        return sorted(set(duplicate_stage_numbers))

    def __init__(self) -> None:
        """
        Подготавливает параметры подключения к исходному отчетному листу и целевой таблице.

        Метод ничего не загружает из Google Sheets сразу.
        Он только сохраняет:
        - откуда читать VED-данные (`report_sheet`);
        - куда писать тестовый объединенный результат.

        Такой ленивый подход полезен для отладки:
        объект можно создать заранее, а фактическое подключение произойдет
        только в момент, когда оно действительно понадобится.
        """
        self._table_name = logistics_ved.get("title")
        self._source_sheet_name = logistics_ved.get("report_sheet")
        self._target_table_name = delivery_calculation_china.get("title")
        self._target_sheet_name = delivery_calculation_china.get("test_sheet")

        self._source_conn: GoogleTabs | None = None
        self._target_connections: dict[str, GoogleTabs] = {}

    @property
    def source_connect(self) -> GoogleTabs:
        """
        Возвращает подключение к исходному листу ВЭД.

        Возвращает:
            Объект `GoogleTabs`, настроенный на таблицу `logistics_ved`.

        Особенность:
            Подключение создается лениво — только при первом обращении.
            Это снижает число лишних подключений и упрощает тестирование.
        """
        if self._source_conn is None:
            self._source_conn = GoogleTabs(
                table_title=self._table_name,
                sheet_title=self._source_sheet_name,
            )
        return self._source_conn

    @property
    def target_connect(self) -> GoogleTabs:
        """
        Возвращает подключение к тестовому листу для выгрузки результата.

        Возвращает:
            Объект `GoogleTabs`, настроенный на `delivery_calculation_china / test_sheet`.

        Бизнес-правило:
            В рамках VED-отладки мы не трогаем production-листы.
            Любая тестовая выгрузка должна идти только в `test_sheet`.
        """
        return self.get_target_connect(self._target_sheet_name)

    def get_target_connect(self, target_sheet_name: str) -> GoogleTabs:
        """
        Возвращает подключение к целевому листу таблицы `delivery_calculation_china`.

        Параметры:
            target_sheet_name:
                Название листа, в который нужно выгрузить подготовленный DataFrame.

        Возвращает:
            Объект `GoogleTabs`, настроенный на переданный лист целевой таблицы.

        Зачем это нужно:
            VED-сервис по-прежнему отвечает только за свою часть данных, но теперь его нужно
            безопасно использовать и для тестовой, и для production-выгрузки без дублирования
            кода подключения.
        """
        if target_sheet_name not in self._target_connections:
            self._target_connections[target_sheet_name] = GoogleTabs(
                table_title=self._target_table_name,
                sheet_title=target_sheet_name,
            )
        return self._target_connections[target_sheet_name]

    @staticmethod
    def normalize_column_name(column_name: object) -> str:
        """
        Приводит название колонки к стабильному виду для последующей проверки.

        Параметры:
            column_name:
                Любое значение, которое пришло из заголовка Google Sheets.

        Возвращает:
            Строку без переносов, табов и лишних пробелов.

        Зачем это нужно:
            в Google Sheets заголовки часто содержат случайные переносы строк
            или двойные пробелы. Без нормализации такие колонки сложно надежно
            сравнивать с `LOGISTICS_VED_REQUIRED_COLUMNS`.
        """
        normalized = str(column_name).replace("\t", " ").replace("\n", " ").replace("\r", " ")
        return re.sub(r"\s+", " ", normalized).strip()

    @staticmethod
    def make_unique_column_names(column_names: list[str]) -> list[str]:
        """
        Делает названия колонок уникальными, если в таблице встретились дубли.

        Параметры:
            column_names:
                Список уже нормализованных заголовков.

        Возвращает:
            Новый список заголовков, где повторы получают числовой суффикс.

        Крайний случай:
            если в Google Sheets одна и та же подпись встречается несколько раз,
            `pandas.DataFrame` без такой обработки будет работать неустойчиво.
        """
        seen_columns: dict[str, int] = {}
        unique_columns: list[str] = []

        for column_name in column_names:
            seen_columns[column_name] = seen_columns.get(column_name, 0) + 1
            if seen_columns[column_name] == 1:
                unique_columns.append(column_name)
                continue

            unique_columns.append(f"{column_name} {seen_columns[column_name]}")

        return unique_columns

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
        """
        Проверяет, что в DataFrame присутствуют все обязательные колонки.

        Параметры:
            df:
                DataFrame, в котором ожидается набор обязательных колонок.
            required_columns:
                Список имен колонок, которые обязательно должны быть найдены.

        Возвращает:
            Ничего. При успехе метод просто завершается.

        Ошибки:
            `ValueError`, если хотя бы одной обязательной колонки нет.

        Почему это важно:
            в VED-логике мы не можем молча продолжать расчет, если отсутствуют
            ключевые поля. Иначе дальше появятся либо ложные пустые значения,
            либо трудноуловимые ошибки на этапе формирования платежей.
        """
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            available_columns = df.columns.tolist()
            raise ValueError(
                "Не найдены обязательные колонки: "
                f"{missing_columns}. Доступные колонки после нормализации: {available_columns}"
            )

    def load_source_data(self) -> pd.DataFrame:
        """
        Загружает исходные данные из таблицы `logistics_ved`.

        Что делает метод:
        - читает значения из Google Sheets;
        - берет строку заголовков из ожидаемой позиции;
        - нормализует названия колонок;
        - создает DataFrame;
        - приводит числовые колонки из `VED_DIGIT_COLS`.

        Возвращает:
            DataFrame с исходными данными ВЭД в широком формате.

        Крайние случаи:
        - если в листе слишком мало строк и строка заголовков не найдена,
          будет выброшен `ValueError`;
        - если какой-то числовой столбец отсутствует, метод не падает, а
          пропускает его, потому что часть колонок может появляться поэтапно.
        """
        # В таблице ВЭД заголовки находятся на третьей строке, а данные идут ниже.
        # Это не "магическое число", а соглашение текущей бизнес-таблицы.
        values = self.source_connect.sheet_title.get_all_values()
        if len(values) < 3:
            raise ValueError("В листе меньше 3 строк, строка заголовков не найдена.")

        headers = self.make_unique_column_names(
            [self.normalize_column_name(header) for header in values[2]]
        )
        df = pd.DataFrame(values[3:], columns=headers)

        # Денежные и количественные поля приходят из Google Sheets как строки.
        # Ниже мы приводим их к числам, чтобы потом можно было корректно
        # агрегировать суммы и разделять оплаченные / неоплаченные значения.
        for column in VED_DIGIT_COLS:
            if column in df.columns:
                df[column] = df[column].apply(clean_currency_value)

        return df

    def prepare_source_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Готовит исходный VED-DataFrame к развороту по этапам платежей.

        Параметры:
            df:
                Исходный широкий DataFrame, загруженный из таблицы ВЭД.

        Возвращает:
            DataFrame только с нужными колонками по поставке, уже переименованными
            в названия, совместимые с будущим `balance_df`.

        Важные преобразования:
        - проверяет наличие `LOGISTICS_VED_REQUIRED_COLUMNS`;
        - проверяет наличие колонок, которые нужны для базового переименования;
        - переименовывает общие поля поставки;
        - добавляет недостающие колонки, которые есть в `balance_df`, но
          отсутствуют в VED-источнике.
        """
        # Сначала убеждаемся, что таблица ВЭД вообще содержит все ключевые поля,
        # без которых дальше невозможно ни построить этапы, ни объяснить результат.
        self.validate_required_columns(df, LOGISTICS_VED_REQUIRED_COLUMNS)
        self.validate_required_columns(df, list(self.BASE_COLUMNS_MAP.keys()))

        df_orders = df.loc[:, LOGISTICS_VED_REQUIRED_COLUMNS].copy()
        df_orders = df_orders.rename(columns=self.BASE_COLUMNS_MAP)

        # `balance_df` из orders_white содержит колонки, которых в ВЭД нет по природе.
        # Мы добавляем их заранее с пустыми значениями, чтобы потом можно было
        # безопасно привести обе структуры к общему формату и объединить через concat.
        for column_name, default_value in self.DEFAULT_COLUMNS.items():
            if column_name not in df_orders.columns:
                df_orders[column_name] = default_value

        return df_orders

    def build_payment_dataframe(
        self,
        df_source: pd.DataFrame,
        df_base: pd.DataFrame,
        payment_config: dict[str, object],
    ) -> pd.DataFrame:
        """
        Формирует строки платежного баланса для одного этапа из `VED_PAYMENT_CONFIGS`.

        Параметры:
            df_source:
                Полный исходный DataFrame ВЭД в широком формате.
            df_base:
                Базовый DataFrame с общими колонками по поставке.
            payment_config:
                Один элемент из `VED_PAYMENT_CONFIGS`.

        Возвращает:
            DataFrame, где каждая строка соответствует выбранному этапу платежа.

        Как работает логика:
        - одна строка исходной таблицы описывает поставку целиком;
        - в этой строке могут лежать сразу несколько видов расходов;
        - поэтому для итоговой аналитики мы "разворачиваем" одну строку поставки
          в несколько строк платежей — по одной строке на каждый этап из конфига.

        Откуда берутся поля этапа:
        - номер этапа платежа: из `payment_config["Номер этапа платежа"]`;
        - название этапа: из `payment_config["Этап платежа"]`;
        - статус этапа, плановая дата, дата платежного календаря и сумма:
          из mapping в `payment_config["columns"]`.

        Почему `VED_PAYMENT_CONFIGS` нельзя считать идентичным
        `ORDERS_WHITE_PAYMENT_CONFIGS`:
        - в VED может не быть части полей, которые есть у orders_white;
        - список ключей в `columns` может отличаться;
        - смысл этапов платежа тоже другой.
        Поэтому ниже мы не делаем универсальных предположений сверх того,
        что явно описано в конфиге.
        """
        payment_columns = payment_config["columns"]
        self.validate_required_columns(df_source, list(payment_columns.values()))

        df_payment = df_base.copy()
        df_payment["_Порядок исходной строки"] = range(len(df_payment))
        df_payment["Этап платежа"] = payment_config["Этап платежа"]
        df_payment["Номер этапа платежа"] = payment_config["Номер этапа платежа"]

        # Ниже переносим данные строго по mapping из `VED_PAYMENT_CONFIGS`.
        # Это важное отличие от "зашитой" логики: если бизнес меняет конфиг,
        # код начинает брать новые поля без переписывания каждого этапа вручную.
        #
        # Отдельно важно помнить про этапы 5 и 6:
        # они могут ссылаться на одинаковые или почти одинаковые исходные поля.
        # Мы не удаляем их автоматически и не меняем бизнес-логику без подтверждения.
        # Если источники совпадают, сервис лишь поднимает диагностику о риске дублей.
        for target_column, source_column in payment_columns.items():
            df_payment[target_column] = df_source[source_column].to_numpy()

        # Часть колонок может не прийти из конкретного этапа VED.
        # Чтобы сохранить совместимость с `balance_df`, ниже добавляем недостающие
        # целевые поля с пустыми значениями.
        if "Сумма_оплаты" not in df_payment.columns:
            df_payment["Сумма_оплаты"] = pd.NA
        if "Дата_план" not in df_payment.columns:
            df_payment["Дата_план"] = pd.NA
        if "Дата_платеж_календарь" not in df_payment.columns:
            df_payment["Дата_платеж_календарь"] = pd.NA
        if "Статус_по_этапу" not in df_payment.columns:
            df_payment["Статус_по_этапу"] = pd.NA

        df_payment["Дата_аванса_по_годовому_плану"] = pd.NA
        df_payment["Дата_факт"] = pd.NA
        df_payment["%_оплаты"] = pd.NA
        return df_payment

    def build_balance_dataframe(self, df_source: pd.DataFrame) -> pd.DataFrame:
        """
        Собирает итоговый `ved_balance_df` по всем этапам из `VED_PAYMENT_CONFIGS`.

        Параметры:
            df_source:
                Исходный DataFrame ВЭД в широком формате.

        Возвращает:
            DataFrame в узком формате, где каждая строка — это отдельный этап платежа.

        Важные бизнес-правила:
        - из одной строки исходной таблицы может получиться несколько строк результата;
        - пустые этапы без статуса отбрасываются;
        - порядок строк сохраняется стабильным, чтобы проще было сверять результат
          с исходной таблицей при отладке.
        """
        df_base = self.prepare_source_dataframe(df_source)

        # Здесь происходит ключевое преобразование VED-таблицы:
        # широкая строка поставки превращается в несколько строк платежного баланса.
        payment_frames = [
            self.build_payment_dataframe(
                df_source=df_source,
                df_base=df_base,
                payment_config=payment_config,
            )
            for payment_config in VED_PAYMENT_CONFIGS
        ]

        df_balance = pd.concat(payment_frames, ignore_index=True)

        # В итоговый баланс берем только те строки, где этап действительно заполнен.
        # Пустой статус почти всегда означает, что конкретный вид платежа
        # для этой поставки отсутствует.
        status_series = df_balance["Статус_по_этапу"].fillna("").astype(str).str.strip()
        df_balance = df_balance[status_series.ne("")].copy()

        return df_balance.sort_values(
            by=["№ проформы в документах и 1С", "_Порядок исходной строки", "Номер этапа платежа"],
            kind="stable",
        ).drop(columns="_Порядок исходной строки").reset_index(drop=True)

    @staticmethod
    def add_payment_status_amounts(df_balance: pd.DataFrame) -> pd.DataFrame:
        """
        Делит сумму платежа на оплаченный и неоплаченный остаток по статусу этапа.

        Параметры:
            df_balance:
                DataFrame с уже сформированными строками платежных этапов.

        Возвращает:
            DataFrame с дополнительными колонками `Оплачено` и `Не_оплачено`.

        Бизнес-правило:
            если `Статус_по_этапу == "оплачено"`, вся сумма этапа идет в колонку
            `Оплачено`, иначе — в `Не_оплачено`.
        """
        df_result = df_balance.copy()
        paid_mask = (
            df_result["Статус_по_этапу"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("оплачен")
        )
        payment_amount = pd.to_numeric(df_result["Сумма_оплаты"], errors="coerce").fillna(0)

        df_result["Оплачено"] = payment_amount.where(paid_mask, 0)
        df_result["Не_оплачено"] = payment_amount.where(~paid_mask, 0)
        return df_result

    @staticmethod
    def align_to_balance_columns(
        ved_balance_df: pd.DataFrame,
        balance_columns: list[str],
    ) -> VedAlignmentResult:
        """
        Приводит `ved_balance_df` к той же структуре колонок, что и `balance_df`.

        Параметры:
            ved_balance_df:
                Готовый VED-результат после расчета этапов платежей.
            balance_columns:
                Список колонок из `balance_df`, порядок которого нужно сохранить.

        Возвращает:
            Объект `VedAlignmentResult` с выровненным DataFrame и диагностикой.

        Что делает метод:
        - ищет недостающие колонки относительно `balance_df`;
        - добавляет их с пустыми значениями;
        - фиксирует лишние колонки из VED;
        - возвращает DataFrame в нужном порядке колонок.
        """
        missing_columns = [column for column in balance_columns if column not in ved_balance_df.columns]
        extra_columns = [column for column in ved_balance_df.columns if column not in balance_columns]

        df_aligned = ved_balance_df.copy()

        # Этот шаг нужен именно для безопасного `pd.concat`.
        # Если просто объединить DataFrame с разным набором колонок, можно
        # получить трудноуловимые перекосы в структуре результата.
        for column in missing_columns:
            df_aligned[column] = pd.NA

        return VedAlignmentResult(
            df_aligned=df_aligned.loc[:, balance_columns],
            missing_columns=missing_columns,
            extra_columns=extra_columns,
        )

    @staticmethod
    def build_duplicate_risk_report(ved_balance_df: pd.DataFrame) -> pd.DataFrame:
        """
        Строит диагностический отчет по потенциальным дублям в VED-платежах.

        Параметры:
            ved_balance_df:
                Итоговый VED-DataFrame после разворота по этапам.

        Возвращает:
            DataFrame только с теми группами строк, где найдено больше одного
            совпадения по ключевым полям этапа.

        Что именно проверяется:
        - одинаковые проформы;
        - одинаковые артикулы и модели;
        - одинаковый статус этапа;
        - одинаковые даты и суммы.

        Почему это важно:
            если разные этапы читают одни и те же исходные поля, итоговые строки
            могут стать одинаковыми. Нормальный результат проверки — пустой отчет.
            Если отчет не пустой, это сигнал для ручной бизнес-проверки.
        """
        risk_columns = [
            "№ проформы в документах и 1С",
            "wild",
            "Модель",
            "Статус_по_этапу",
            "Дата_план",
            "Дата_платеж_календарь",
            "Сумма_оплаты",
        ]
        available_columns = [column for column in risk_columns if column in ved_balance_df.columns]
        if not available_columns:
            return pd.DataFrame()

        # Смотрим не на все этапы подряд, а только на те, у которых конфиг сам по себе
        # выглядит подозрительно: один и тот же набор исходных колонок используется
        # для нескольких разных номеров этапов.
        duplicate_stage_numbers = VedBalanceAnalyticsService.get_duplicate_risk_stage_numbers()
        if not duplicate_stage_numbers:
            return pd.DataFrame(columns=available_columns + ["Количество дублей"])

        df_risk = ved_balance_df[
            ved_balance_df["Номер этапа платежа"].isin(duplicate_stage_numbers)
        ].copy()
        if df_risk.empty:
            return pd.DataFrame(columns=available_columns + ["Количество дублей"])

        return (
            df_risk.groupby(available_columns, dropna=False)
            .size()
            .reset_index(name="Количество дублей")
            .query("`Количество дублей` > 1")
            .sort_values("Количество дублей", ascending=False)
            .reset_index(drop=True)
        )

    @staticmethod
    def prepare_dataframe_for_upload(df_balance: pd.DataFrame) -> pd.DataFrame:
        """
        Добавляет служебные колонки перед выгрузкой в Google Sheets.

        Параметры:
            df_balance:
                DataFrame, который уже готов к выгрузке.

        Возвращает:
            Копию исходного DataFrame с колонками `Месяц` и `updated_at`.
        """
        df_upload = df_balance.copy()
        payment_calendar_dates = pd.to_datetime(
            df_upload["Дата_платеж_календарь"],
            errors="coerce",
            dayfirst=True,
            format="mixed",
        )
        df_upload["Месяц"] = payment_calendar_dates.dt.strftime("%m-%Y").fillna("")
        df_upload[ORDERS_WHITE_UPDATED_AT_COLUMN] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df_upload

    def upload_to_test_sheet(self, df_upload: pd.DataFrame) -> None:
        """
        Выгружает результат только в тестовый лист `delivery_calculation_china / test_sheet`.

        Параметры:
            df_upload:
                DataFrame, который уже выровнен и готов к загрузке.

        Возвращает:
            Ничего.

        Бизнес-ограничение:
            production-листы здесь не трогаются. Этот метод существует именно для
            безопасной отладки нового VED-пайплайна.
        """
        self.upload_to_sheet(
            df_upload=df_upload,
            target_sheet_name=self._target_sheet_name,
        )

    def upload_to_sheet(self, df_upload: pd.DataFrame, target_sheet_name: str) -> None:
        """
        Выгружает подготовленный DataFrame в указанный лист таблицы `delivery_calculation_china`.

        Параметры:
            df_upload:
                DataFrame, уже готовый к записи в Google Sheets.
            target_sheet_name:
                Название целевого листа в таблице `delivery_calculation_china`.

        Возвращает:
            Ничего.

        Ограничение:
            Метод не меняет структуру DataFrame и не решает, что именно выгружать.
            Он отвечает только за запись уже подготовленных данных в нужный лист.
        """
        if df_upload.empty:
            logger.warning(
                "combined_balance_df пустой. Выгрузка в лист %s пропущена.",
                target_sheet_name,
            )
            return

        self.get_target_connect(target_sheet_name).set_df_to_google(df_upload)
        logger.info("combined_balance_df выгружен на лист %s.", target_sheet_name)

    def run(self) -> pd.DataFrame:
        """
        Выполняет полный VED-расчет без объединения с `balance_df`.

        Возвращает:
            `ved_balance_df` — DataFrame с разложенными по этапам VED-платежами.

        Pipeline метода:
        1. загрузка широких данных ВЭД;
        2. подготовка и валидация;
        3. разворот по `VED_PAYMENT_CONFIGS`;
        4. расчет колонок `Оплачено` и `Не_оплачено`.
        """
        df_source = self.load_source_data()
        df_balance = self.build_balance_dataframe(df_source)
        return self.add_payment_status_amounts(df_balance)
