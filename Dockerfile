# =============================================================================
# Dockerfile - Worker de Mensageria SIFEN
# =============================================================================
# Usa python-oracledb em thick mode (com Oracle Instant Client).
# Necessário para suportar password verifier SHA512 (tipo 0x939).
# =============================================================================

FROM python:3.10-slim-bookworm

LABEL maintainer="GSF Soluções"
LABEL description="Worker de mensageria SIFEN - Processamento de faturas eletrônicas"

# Evita criação de arquivos .pyc e habilita output sem buffer (logs em tempo real)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# --- Thick Mode (Oracle Instant Client) - Necessário para password verifier SHA512 ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends libaio1 wget unzip && \
    mkdir -p /opt/oracle && cd /opt/oracle && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basiclite-linux.x64-23.4.0.24.05.zip && \
    unzip instantclient-basiclite-linux.x64-23.4.0.24.05.zip && \
    rm -f *.zip && \
    apt-get purge -y wget unzip && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_23_4:$LD_LIBRARY_PATH
# --- Fim Thick Mode ---

# Copia e instala dependências primeiro (cache de camadas Docker)
COPY requirements.txt /app/messaging_standalone/requirements.txt
RUN pip install --no-cache-dir -r /app/messaging_standalone/requirements.txt

# Copia código da aplicação
COPY . /app/messaging_standalone/

# Inicia o worker
CMD ["python", "-m", "messaging_standalone.worker"]
