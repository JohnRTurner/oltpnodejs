#Deriving the latest base image
FROM node:18
LABEL Maintainer="johnrturner"
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install
COPY . .

CMD [ "node", "bin/run.js" ]