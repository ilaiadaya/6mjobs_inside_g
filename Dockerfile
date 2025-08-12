# Dockerfile
FROM node:20-slim

WORKDIR /app

# Copy only manifests, install prod deps
COPY package*.json ./
RUN npm install --omit=dev

# Copy the rest
COPY . .

# Run your script
CMD ["node", "sync.js"]
