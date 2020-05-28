FROM python:3.6

ENV WD /
ENV NEOFSCLI_VERSION '0.8.0'

RUN apt-get update \
    && pip3 install robotframework==3.2.1

RUN cd /tmp \
    && wget https://github.com/nspcc-dev/neofs-cli/releases/download/v${NEOFSCLI_VERSION}/neofs-cli_${NEOFSCLI_VERSION}_linux_x86_64.tar.gz \
    && tar xfz neofs-cli_${NEOFSCLI_VERSION}_linux_x86_64.tar.gz \
    && mv neofs-cli /usr/local/bin \
    && rm -rf /tmp/neofs-cli

WORKDIR ${WD}
COPY ./ ${WD}
