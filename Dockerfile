FROM docker:19.03.11-dind

ENV WD /
ARG REG_USR
ARG REG_PWD
ARG JF_TOKEN
ARG BUILD_NEOFS_NODE
ENV REG_USR=${REG_USR}
ENV REG_PWD=${REG_PWD}
ENV NEOFSCLI_VERSION=0.9.0
ENV JF_TOKEN=${JF_TOKEN}
ENV BUILD_NEOFS_NODE=${BUILD_NEOFS_NODE}

ENV RF_VERSION 3.2.1

RUN apk add --no-cache openssh
RUN apk add --no-cache libressl-dev
RUN apk add --no-cache curl
RUN apk add --no-cache bash bash-doc bash-completion
RUN apk add --no-cache util-linux pciutils usbutils coreutils binutils findutils grep gcc libffi-dev openssl-dev 
RUN apk add --no-cache sudo

RUN apk --no-cache add \
        make \
        python3 \
        py3-pip 

RUN apk --no-cache add --virtual \
        .build-deps \
        build-base \
        python3-dev 

RUN addgroup nobody root && \
    echo "export PYTHONPATH=\$PYTHONPATH:/.local/lib/python3.8/site-packages" > /.profile && \
    mkdir -p /tests /reports /.local && \
    chgrp -R 0 /reports /.local && \
    chmod -R g=u /etc/passwd /reports /.local /.profile

RUN pip3 install wheel
RUN pip3 install robotframework
RUN pip3 install pexpect
RUN pip3 install requests


# Golang
ARG GOLANG_VERSION=1.14.3
#we need the go version installed from apk to bootstrap the custom version built from source
RUN apk update && apk add go gcc bash musl-dev openssl-dev ca-certificates && update-ca-certificates
RUN wget https://dl.google.com/go/go$GOLANG_VERSION.src.tar.gz && tar -C /usr/local -xzf go$GOLANG_VERSION.src.tar.gz
RUN cd /usr/local/go/src && ./make.bash
ENV PATH=$PATH:/usr/local/go/bin
RUN rm go$GOLANG_VERSION.src.tar.gz
#we delete the apk installed version to avoid conflict
RUN apk del go
RUN go version


# Add the keys and set permissions
COPY    ./ca/*    /root/.ssh/

RUN chmod 600 /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa.pub

RUN pip3 install docker-compose

RUN export DOCKER_HOST="${HOSTNAME}-docker"
RUN apk add --no-cache git \
    --repository https://alpine.global.ssl.fastly.net/alpine/v3.10/community \
    --repository https://alpine.global.ssl.fastly.net/alpine/v3.10/main

RUN mkdir -p /robot/vendor

RUN cd /robot/vendor \
    && git clone https://github.com/nspcc-dev/neofs-dev-env.git

WORKDIR ${WD}
COPY ./ ${WD}

RUN cd ${WD} && chmod +x dockerd.sh
