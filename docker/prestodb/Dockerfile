ARG BASE_OS
FROM amazonlinux:2

ARG PRESTO_VERSION
ARG PRESTO_PKG=presto-server-$PRESTO_VERSION.tar.gz
ARG PRESTO_CLI_JAR=presto-cli-$PRESTO_VERSION-executable.jar

ENV PRESTO_HOME="/opt/presto-server"

COPY build-artifacts/$PRESTO_PKG .
COPY build-artifacts/$PRESTO_CLI_JAR /opt/presto-cli

RUN yum install tar gzip java-1.8.0-openjdk.x86_64 less -y \
	# Download Presto and move \
	&& tar -zxf $PRESTO_PKG \
	&& mv ./presto-server-$PRESTO_VERSION $PRESTO_HOME \
	&& rm -rf $PRESTO_PKG \
	&& rm -rf ./presto-server-$PRESTO_VERSION \
	&& chmod +x /opt/presto-cli \
	&& ln -s /opt/presto-cli /usr/local/bin/ \
	# clean cache jobs
	&& yum remove -y tar \
	&& yum clean all \
	#  mkdir for config
	&& mkdir -p $PRESTO_HOME/etc \
	&& mkdir -p $PRESTO_HOME/etc/catalog \
	&& mkdir -p /var/lib/presto/data

COPY ./docker/prestodb/etc $PRESTO_HOME/etc
COPY ./docker/prestodb/entrypoint.sh /opt

ENTRYPOINT ["/opt/entrypoint.sh"]
