FROM python:3.13-slim

# Static ffmpeg from BtbN (ffmpeg.org's linked provider). Debian
# Trixie freezes at 7.1.x; we want 8.1+ for HLS PDT fixes and the
# native AAC improvements. The -latest link rebuilds nightly so a
# `docker compose build` pulls whatever is current.
#
# Comskip used to be a build stage above this — fully removed
# 2026-04-25 after the live-skip path also migrated to tv-detect.
# tv-detect runs externally (Mac via tv-comskip.sh + tv-live-comskip.py)
# under LIVE_ADS_OFFLOAD=mac and is mounted into this container at
# /usr/local/bin/tv-detect via docker-compose.
ARG FFMPEG_URL=https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-n8.1-latest-linuxarm64-gpl-8.1.tar.xz

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl xz-utils ca-certificates \
        libavutil59 libavcodec61 libavformat61 \
        libswscale8 libswresample5 && \
    curl -fL "$FFMPEG_URL" -o /tmp/ffmpeg.tar.xz && \
    tar -xJf /tmp/ffmpeg.tar.xz -C /tmp && \
    cp /tmp/ffmpeg-*/bin/ffmpeg  /usr/local/bin/ffmpeg  && \
    cp /tmp/ffmpeg-*/bin/ffprobe /usr/local/bin/ffprobe && \
    rm -rf /tmp/ffmpeg-* /tmp/ffmpeg.tar.xz && \
    apt-get purge -y --auto-remove curl xz-utils && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir flask waitress

COPY service.py /app/service.py
WORKDIR /app

EXPOSE 8080
CMD ["python", "-u", "service.py"]
