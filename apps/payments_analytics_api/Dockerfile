FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY apps/payments_analytics_api/requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

RUN mkdir -p \
    app \
    src_oop/core \
    src_oop/jobs/calculation_of_purchases_china

COPY apps/payments_analytics_api/app ./app
COPY src_oop/__init__.py ./src_oop/__init__.py
COPY src_oop/core/my_gspread.py ./src_oop/core/my_gspread.py
COPY src_oop/core/utils_general.py ./src_oop/core/utils_general.py
COPY src_oop/jobs/calculation_of_purchases_china/config.py ./src_oop/jobs/calculation_of_purchases_china/config.py
COPY src_oop/jobs/calculation_of_purchases_china/orders_white_balance_analytics.py ./src_oop/jobs/calculation_of_purchases_china/orders_white_balance_analytics.py

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
