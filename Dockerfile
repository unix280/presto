FROM maven:3-openjdk-8 as builder

WORKDIR /prestodb
COPY . /prestodb

# Skip everything we don't need in the dist
RUN mvn -DskipTests=true -T 4 -Dair.check.skip-all=true -Dmaven.javadoc.skip=true \
    -pl '!presto-docs,!presto-tests,!presto-testing-docker,!presto-testing-server-launcher,!presto-product-tests,!presto-server-rpm,!presto-spark-testing' \
    package

RUN mkdir -p presto-dist
RUN tar xf presto-server/target/presto-server-*.tar.gz -C presto-dist --strip-components=1

FROM openjdk:8-jre

# The launcher.py script is writter in Python
RUN apt-get update && apt-get install -yqq python && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /prestodb

COPY --from=builder /prestodb/presto-dist /prestodb

CMD /prestodb/bin/launcher run
