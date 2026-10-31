FROM python:3.11-slim

WORKDIR /app

COPY ai_serving/requirements.txt ./ai_serving/requirements.txt

RUN pip install --no-cache-dir -r ai_serving/requirements.txt

COPY ai_serving ./ai_serving

EXPOSE 8000

CMD ["uvicorn", "ai_serving.app:app", "--host", "0.0.0.0", "--port", "8000"]