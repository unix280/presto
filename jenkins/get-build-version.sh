#!/usr/bin/env bash
set -e

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
cd ${SCRIPT_DIR}/..

PRESTO_VERSION=${PRESTO_VERSION:-$(MAVEN_CONFIG="" && ./mvnw org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout)}
export COMMIT_ID=$(git rev-parse HEAD)
BUILD_VERSION=${PRESTO_VERSION}-$(git show -s --format=%cd --date=format:"%Y%m%d" ${COMMIT_ID})-$(git rev-parse --short=7 HEAD)
echo ${BUILD_VERSION}
