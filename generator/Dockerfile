# Build context = raiz do repo (ver docker-compose.yml) para acessar ./contracts.
FROM python:3.12-slim

WORKDIR /app
COPY generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator/simulator.py .
COPY contracts/ ./contracts/

CMD ["python", "-u", "simulator.py"]
