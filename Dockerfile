ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

# Install Python dependencies
RUN apk add --no-cache python3 py3-pip && \
    pip3 install --no-cache-dir --break-system-packages \
        aiohttp \
        flask \
        waitress

# Copy source files into /app/
WORKDIR /app
COPY . /app/

# Ensure run.sh is executable and has Unix line endings
RUN sed -i 's/\r//' /app/run.sh && \
    chmod +x /app/run.sh

CMD ["/app/run.sh"]