# Stage 0: Base Stage - For installing dependencies once
FROM python:3.10-slim AS base

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements file to leverage Docker cache
COPY requirements.txt .

# Install all dependencies
RUN pip install --no-cache-dir -r requirements.txt

# ---
# Stage 1: Build the Scheduler Service
# ---
FROM base AS scheduler

# Copy the models and other application files for this service.
COPY models/ models/
COPY main.py .
COPY config.py .
COPY services/ services/
COPY utils/ utils/

# Set the command to run the application
# Assuming scheduler is a standalone script
CMD ["python", "scheduler.py"]

# ---
# Stage 2: Build the TGJU Proxy Service
# ---
FROM base AS tgju-proxy

# Copy only the specific code files for this service
COPY services/tgju.py services/
COPY main.py .

# Expose the port 5001 for the TGJU proxy
EXPOSE 5001

# Run the TGJU proxy service
CMD ["python", "main.py"]

# ---
# Stage 3: Build the API Service
# ---
FROM base AS api

# Copy all the application files needed for the API service
COPY main.py .
COPY config.py .
COPY services/ services/
COPY utils/ utils/
COPY models/ models/
COPY routes/ routes/
COPY models.py .
COPY extensions.py .
COPY ml_predictor.py .

# Expose port 5000
EXPOSE 5000

# Run the API service
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "main:create_app()"]