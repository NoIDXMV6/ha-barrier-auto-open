FROM python:3.12-slim

# ffmpeg — единственная системная зависимость (кодирование H.264 на CPU)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py get_egds_camera_creds.py ./

ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py", "/config/config.yaml"]
