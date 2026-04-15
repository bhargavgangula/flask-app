FROM python:3.10-slim

# Install system deps required by Selenium / Chrome
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget gnupg curl unzip \
        libnss3 libxss1 libasound2 libatk-bridge2.0-0 libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["gunicorn", "Flask_Scraper_Backend:app", "--bind", "0.0.0.0:5000"]
