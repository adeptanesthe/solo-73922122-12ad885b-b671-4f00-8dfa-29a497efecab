FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HANDOFF_DATA_DIR=/data

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --system --uid 10001 appuser \
    && mkdir -p /data/files /data/tmp \
    && chown -R appuser:appuser /data /srv
USER appuser

EXPOSE 8000

# 每个容器一个 Web 进程；compose 起两个容器共同挂载同一数据卷
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
