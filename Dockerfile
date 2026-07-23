# Stage 1: install dependencies
FROM node:20-slim AS deps

RUN apt-get update && apt-get install -y \
    python3 ffmpeg make g++ build-essential \
    openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json ./
COPY prisma ./prisma

RUN yarn install --frozen-lockfile --ignore-engines --network-timeout 300000

# Stage 2: runtime
FROM node:20-slim AS runner

RUN apt-get update && apt-get install -y \
    python3 ffmpeg openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV NODE_ENV=production

COPY --from=deps /app/node_modules ./node_modules
COPY --from=deps /app/prisma ./prisma
COPY sources ./sources
COPY package.json ./
COPY tsconfig.json ./

EXPOSE 3000

# Keep migration and runtime startup sequential, then replace PID 1 with the
# actual Node/tsx process so Docker SIGTERM reaches Happy's shutdown handlers.
CMD ["sh", "-c", "npx prisma migrate deploy && exec ./node_modules/.bin/tsx ./sources/main.ts"]
