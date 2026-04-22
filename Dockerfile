FROM python:3.12-slim
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/main.py .
COPY static/ /app/static/
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8196"]
