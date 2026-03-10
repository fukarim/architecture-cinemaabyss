FROM node:22-alpine

WORKDIR /app

COPY package.json .
COPY package-lock.json .
RUN npm ci

COPY . .

EXPOSE 8082

# Start the application
CMD ["node", "index.js"]
