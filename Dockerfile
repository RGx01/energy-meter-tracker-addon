ARG BUILD_FROM=ghcr.io/home-assistant/base:latest
FROM $BUILD_FROM
 
# System Python
RUN apk add --no-cache python3 py3-pip
 
# Python deps first — this layer is cached unless requirements.txt changes,
# so day-to-day source edits no longer reinstall aiohttp/flask/waitress.
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt
 
# App source
WORKDIR /app
COPY . /app/
RUN sed -i 's/\r//' /app/run.sh && chmod +x /app/run.sh
 
CMD ["/app/run.sh"]
 