ARG BASE_PRIVATE_REGISTRY=""
FROM ${BASE_PRIVATE_REGISTRY}hkube/base-node:v1.2.0
LABEL maintainer="yehiyam@gmail.com"
RUN mkdir /hkube
COPY . /hkube/storage-cleaner
RUN cd /hkube/storage-cleaner
WORKDIR /hkube/storage-cleaner
CMD ["node", "app.js"]
