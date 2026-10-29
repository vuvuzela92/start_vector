-- Значения в уведомлениях о выкупе WB могут быть длиннее 255 символов.
-- Изменение безопасно для исторических данных: типы расширяются без обрезки.
ALTER TABLE redeem_notification
    ALTER COLUMN "№" TYPE TEXT,
    ALTER COLUMN wild TYPE TEXT,
    ALTER COLUMN subject_name TYPE TEXT,
    ALTER COLUMN kiz TYPE TEXT,
    ALTER COLUMN doc_name TYPE TEXT,
    ALTER COLUMN account TYPE TEXT;
