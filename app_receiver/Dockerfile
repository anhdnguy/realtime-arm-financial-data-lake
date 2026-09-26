FROM python:3.13-slim

WORKDIR /app

# Copy requirements first (Docker layer caching optimization)
# If requirements.txt doesn't change, this layer is cached
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . ./app/

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set Python to run in unbuffered mode (see logs immediately)
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "-m", "app.test"]