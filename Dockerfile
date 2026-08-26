FROM python:3.12-slim

# ffmpeg для конвертации, curl для скачивания deno
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg curl unzip && \
    rm -rf /var/lib/apt/lists/*

# deno — JS-рантайм для yt-dlp (решает YouTube n-challenge → 720p)
RUN curl -fsSL https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip \
    -o /tmp/deno.zip && \
    unzip /tmp/deno.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/deno && \
    rm /tmp/deno.zip && \
    # без рабочего deno yt-dlp не решит n-challenge и вернёт пустой список форматов —
    # пусть это ломает сборку, а не всплывает потом как "Requested format is not available"
    deno --version

WORKDIR /app

# сначала зависимости (кэшируется Docker слоем)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# потом код
COPY bot/ bot/

CMD ["python", "-m", "bot.main"]
