ARG BASE_OS
FROM $BASE_OS

RUN yum install tar gzip java-1.8.0-openjdk.x86_64 less -y

ARG PRESTO_VERSION
ARG PRESTO_PKG=presto-server-$PRESTO_VERSION.tar.gz

COPY build-artifacts/$PRESTO_PKG .
RUN tar -zxf $PRESTO_PKG

ENV PRESTO_HOME="/opt/presto-server"
RUN mv presto-server-$PRESTO_VERSION $PRESTO_HOME

# Create directory for Presto data
RUN mkdir -p $PRESTO_HOME/etc \
	&& mkdir -p $PRESTO_HOME/etc/catalog \
	&& mkdir -p /var/lib/presto/data

COPY ./docker/prestodb/etc $PRESTO_HOME/etc
COPY ./docker/prestodb/entrypoint.sh /opt

# Copy Presto CLI
ARG PRESTO_CLI_JAR=presto-cli-$PRESTO_VERSION-executable.jar
COPY build-artifacts/$PRESTO_CLI_JAR /opt/presto-cli
RUN chmod +x /opt/presto-cli \
    && ln -s /opt/presto-cli /usr/local/bin/

ENTRYPOINT ["/opt/entrypoint.sh"]
