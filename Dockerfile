# Stage 0: Base Stage
FROM python:3.10-slim AS base
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Scheduler ---
FROM base AS scheduler
COPY main.py .
COPY config.py .
COPY services/ services/
COPY utils/ utils/
# مدل‌ها با volume mount استفاده می‌شوند
CMD ["python", "scheduler.py"]

# --- TGJU Proxy ---
FROM base AS tgju-proxy
COPY services/tgju.py services/
COPY main.py .
EXPOSE 5001
CMD ["python", "main.py"]

# --- API ---
FROM base AS api
COPY main.py .
COPY config.py .
COPY services/ services/
COPY utils/ utils/
COPY routes/ routes/
COPY models.py .
COPY extensions.py .
COPY ml_predictor.py .
# مدل‌ها با volume mount استفاده می‌شوند
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "main:create_app()"]
