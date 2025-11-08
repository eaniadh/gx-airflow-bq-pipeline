FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Put your app at /app
WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source (main.py, planner.py, infra/, etc.)
COPY . .

# Ensure /app is on the module path
ENV PYTHONPATH=/app

# Cloud Run sets $PORT. Bind to it.
# Use sh -c so ${PORT} expands.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
