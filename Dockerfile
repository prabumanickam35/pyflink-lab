FROM flink:latest


# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3.10 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get -y install --no-install-recommends apt-utils 2>&1 \
    && apt-get install build-essential -y \
    && apt-get -y install curl \
    && apt-get install openssh-server -y \
    && curl -sSL https://install.python-poetry.org | POETRY_HOME=/etc/poetry python3 - \
    && echo "export PATH=/"$PATH:/etc/poetry/bin"" >> /etc/profile \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/heart_rate_flink/src
RUN mkdir -p /opt/heart_rate_flink/data
# RUN mkdir -p /opt/heart_rate_flink/data/output/starter_result
# RUN chmod 777 /opt/heart_rate_flink/data/output/starter_result

WORKDIR /opt/heart_rate_flink
COPY *.toml /opt/heart_rate_flink/
COPY data/input/ /opt/heart_rate_flink/data/input/
RUN pwd && ls
RUN /etc/poetry/bin/poetry install 
COPY src/*.py /opt/heart_rate_flink/src/