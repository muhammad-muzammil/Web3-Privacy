FROM node:20-slim

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    xvfb \
    libcups2 \
    libxfixes3 \
    libnss3 \
    libgbm1 \
    libatk-bridge2.0-0 \
    libxkbcommon0 \
    libasound2 \
    libcairo2 \
    libpango-1.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    fonts-liberation \
    fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /crawler

# Install dependencies (Puppeteer will download its own Chromium)
COPY package.json package-lock.json* ./
RUN npm install --production

# Copy crawler source
COPY chrome/ ./chrome/
COPY kafka-crawl.js .
COPY match.js .
COPY mongodb.js .
COPY run.js .

# Token match configuration (consumed by match.js -> loadConfig at startup)
COPY crawler_search_terms.json .
COPY crawler_false_flags.json .

# Bundle MetaMask extension
COPY metamask-chrome-10.22.2/ ./metamask-chrome-10.22.2/

ENV TZ=America/New_York
ENV DISPLAY=:99
ENV METAMASK_PATH=./metamask-chrome-10.22.2

# Start xvfb (needed for extensions) then run the crawler
CMD ["sh", "-c", "Xvfb :99 -screen 0 1920x1080x24 & node kafka-crawl.js"]
