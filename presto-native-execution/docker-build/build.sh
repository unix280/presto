#!/bin/bash -ex

AWS_PROFILE=${AWS_PROFILE:-default}
AWS_REGION="us-east-1"
AWS_ECR="299095523242.dkr.ecr.$AWS_REGION.amazonaws.com"
DOCKER_REPO="${AWS_ECR}/ahanaio/devops/prestocpp-builder"
DOCKER_TAG="$(TZ=UTC date +%Y%m%d)H$(git rev-parse --short=7 HEAD)"
DOCKER_IMAGE="${DOCKER_REPO}:${DOCKER_TAG}"
printenv | sort

REPO_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/.."
cd ${REPO_DIR}/
git submodule init && git submodule update

cd docker-build/
mkdir -p cpp/ && cp -r  ../scripts cpp/
docker buildx build --load --platform linux/amd64 --progress plain -t ${DOCKER_IMAGE} .
rm -fr cpp/
aws ecr get-login-password --region ${AWS_REGION} --profile ${AWS_PROFILE} | docker login --username AWS --password-stdin ${AWS_ECR}
docker push ${DOCKER_IMAGE}
 