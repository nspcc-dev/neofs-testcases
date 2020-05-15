FROM python:3.6

ENV WD /testcases/
ENV NEOFSCLI_VERSION '0.7.1'

RUN apt-get update \
    && pip3 install robotframework==3.2.1

#RUN cd /tmp \
#    && wget https://github.com/nspcc-dev/neofs-cli/releases/download/v0.7.1/neofs-cli_${NEOFSCLI_VERSION}_linux_x86_64.tar.gz \
#    && tar xfz neofs-cli_${NEOFSCLI_VERSION}_linux_x86_64.tar.gz \
#    && mv neofs-cli /usr/local/bin

WORKDIR ${WD}
COPY ./ ${WD}
# temporary hack due to slow github servers
RUN mv ${WD}/neofs-cli /usr/local/bin
