FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# No default CMD -- docker-compose.yml and the k8s manifests each set
# the right command for producer vs. consumer from this same image.
