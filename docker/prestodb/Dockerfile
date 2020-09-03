ARG BASE_OS
FROM $BASE_OS

RUN yum install tar gzip java-1.8.0-openjdk.x86_64 -y

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
ENTRYPOINT ["/opt/entrypoint.sh"]
