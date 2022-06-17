ARG ARCH

FROM python:3.9-slim

ARG ARCH

RUN apt-get -y update && apt-get -y install \
    gcc                         \
    make                        \
    git                         \
    curl                        \
    wget                        \
    openssh-client              \
    iputils-ping                \
    unzip                       \
    vim                         \
    dbus                        \
    lsof                        \
    tini                        \
    libssl-dev                  \
    expect                      \
    runc                        \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY requirements.txt ./

# install aws cli utility v2 from pip
RUN python3 -m pip install awscliv2
RUN ln /usr/local/bin/awscliv2 /usr/bin/aws
RUN awsv2 --install
RUN aws --version

RUN pip install --no-cache-dir -r requirements.txt

ENV     PATH=$PATH:/root:/neofs

WORKDIR /root

ENTRYPOINT ["tini", "--", "pytest"]