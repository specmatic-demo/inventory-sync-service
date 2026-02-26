FROM node:24

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY src ./src
RUN git clone https://github.com/specmatic-demo/central-contract-repository /app/.specmatic/repos/central-contract-repository

ENV INVENTORY_SYNC_HOST=0.0.0.0
ENV INVENTORY_SYNC_PORT=9011

EXPOSE 9011
CMD ["npm", "run", "start"]
