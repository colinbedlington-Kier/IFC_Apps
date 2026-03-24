FROM python:3.13-slim

WORKDIR /app

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt packages.txt ./

RUN apt-get update && \
    awk '{ sub(/[[:space:]]*#.*/, ""); gsub(/^[[:space:]]+|[[:space:]]+$/, ""); if (length) print }' packages.txt > /tmp/apt-packages-clean.txt && \
    awk '!/^[a-z0-9][a-z0-9+.-]*$/ { print "Invalid apt package entry in packages.txt: " $0; bad=1 } END { exit bad }' /tmp/apt-packages-clean.txt && \
    if [ -s /tmp/apt-packages-clean.txt ]; then \
        xargs -r -a /tmp/apt-packages-clean.txt apt-get install -y --no-install-recommends; \
    fi && \
    apt-get install -y --no-install-recommends curl ca-certificates gnupg bash && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /app/scripts/bootstrap_cobieqc.sh

EXPOSE 7860

CMD ["bash", "-lc", "/app/scripts/bootstrap_cobieqc.sh && uvicorn app:app --host 0.0.0.0 --port 7860"]
