FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

WORKDIR /app

# Copy requirements from project root and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy .env file from project root
COPY .env .

# Copy new_coin_listing application code
COPY App/new_coin_listing/ .

# Run the application
CMD ["python", "start.py"]
