FROM python:3.11-slim
# Note: Hugging Face Spaces deploy for this repo is configured as `sdk: gradio` (non-Docker),
# so `packages.txt` is the authoritative place for system packages in HF runtime.

WORKDIR /app
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 7860
CMD ["bash", "-lc", "scripts/bootstrap_cobieqc.sh && uvicorn app:app --host 0.0.0.0 --port 7860"]
