FROM node:20-slim

WORKDIR /app

COPY package.json ./
COPY scale-screening-web-app ./scale-screening-web-app

ENV NODE_ENV=production
ENV HOST=0.0.0.0

CMD ["npm", "start"]
