ARG BASE_OS
FROM $BASE_OS

ARG PRESTO_VERSION
ARG PRESTO_CLI_JAR=presto-cli-$PRESTO_VERSION-executable.jar

RUN yum install java-11-amazon-corretto less -y

# Copy Presto CLI
COPY build-artifacts/$PRESTO_CLI_JAR /opt/presto-cli

RUN chmod +x /opt/presto-cli \
    && ln -s /opt/presto-cli /usr/local/bin/
