# Use official Playwright image with Python
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Set working directory
WORKDIR /app

# Copy requirements first (Docker layer caching optimization)
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install Playwright browsers after pip install
RUN playwright install chromium
RUN playwright install-deps  # Essential: Without this, browser won't launch in Docker

# Copy source code
COPY . .

# Expose port (if needed for web interface)
EXPOSE 8000

# Default command (we'll create this file next)
CMD ["python", "src/main.py"]
