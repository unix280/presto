pipeline {

    agent none

    environment {
        AWS_CREDENTIAL_ID  = 'aws-jenkins'
        AWS_DEFAULT_REGION = 'us-east-1'
        AWS_ECR            = '299095523242.dkr.ecr.us-east-1.amazonaws.com'
        AWS_S3_PREFIX      = 's3://ahana-jenkins/artifact/prestodb'
        DOCKER_PLATFORM    = 'linux/amd64'
    }

    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '500'))
        timeout(time: 2, unit: 'HOURS')
    }

    parameters {
        booleanParam(name: 'PUBLISH_ARTIFACTS_ON_CURRENT_BRANCH',
                     defaultValue: false,
                     description: 'artifacts built with branch develop are always published,\n' +
                                  'for any other branch/PR, you need to check this to publish artifacts'
        )
    }

    stages {
        stage('Maven') {
            agent {
                kubernetes {
                    defaultContainer 'maven'
                    yamlFile 'jenkins/agent-maven.yaml'
                }
            }

            stages {
                stage('Maven Setup') {
                    steps {
                        sh 'apt update && apt install -y awscli git tree'
                        sh 'unset MAVEN_CONFIG && ./mvnw versions:set -DremoveSnapshot'

                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     'maven-s3-private-repo',
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''
                                sed -i "s#MAVEN_S3_ACCESS_KEY#$AWS_ACCESS_KEY_ID#g" settings.xml
                                sed -i "s#MAVEN_S3_SECRET_KEY#$AWS_SECRET_ACCESS_KEY#g" settings.xml
                                cat settings.xml
                            '''
                        }
                        script {
                            env.PRESTO_VERSION = sh(
                                script: 'unset MAVEN_CONFIG && ./mvnw org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -q -DforceStdout',
                                returnStdout: true).trim()
                            env.PRESTO_PKG = "presto-server-${PRESTO_VERSION}.tar.gz"
                            env.PRESTO_CLI_JAR = "presto-cli-${PRESTO_VERSION}-executable.jar"
                            env.PRESTO_BUILD_VERSION = env.PRESTO_VERSION + '-' +
                                                    sh(script: 'git rev-parse --short=7 HEAD', returnStdout: true).trim()
                            env.DOCKER_IMAGE = env.AWS_ECR + "/ahanaio/prestodb:${PRESTO_BUILD_VERSION}"
                            env.DOCKER_IMAGE_LATEST = env.AWS_ECR + "/ahanaio/prestodb:latest"
                            env.DOCKER_NATIVE_IMAGE = env.AWS_ECR + "/ahanaio/prestodb-native:${PRESTO_BUILD_VERSION}"
                            env.DOCKER_NATIVE_IMAGE_LATEST = env.AWS_ECR + "/ahanaio/prestodb-native:latest"
                        }
                        sh 'printenv | sort'
                    }
                }

                stage('Maven Build') {
                    steps {
                        echo "build prestodb source code with build version ${PRESTO_BUILD_VERSION}"
                        sh '''
                            unset MAVEN_CONFIG && ./mvnw install -DskipTests -B -T C1 -P ci -pl '!presto-docs'
                            tree /root/.m2/repository/com/facebook/presto/
                        '''

                        echo 'Publish Maven tarball'
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''
                                aws s3 cp presto-server/target/${PRESTO_PKG}  ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
                                aws s3 cp presto-cli/target/${PRESTO_CLI_JAR} ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
                            '''
                        }
                    }
                }
            }
        }

        stage('Docker') {
            agent {
                kubernetes {
                    defaultContainer 'dind'
                    yamlFile 'jenkins/agent-dind.yaml'
                }
            }

            stages {
                stage('Docker Setup') {
                    steps {
                        sh 'apk update && apk add aws-cli bash git tree'
                        sh '''
                            for dir in /home/jenkins/agent/workspace/*/; do
                                echo "${dir}"
                                git config --global --add safe.directory "${dir:0:-1}"
                            done
                        '''
                    }
                }

                stage('Docker Build') {
                    steps {
                        echo "Building ${DOCKER_IMAGE}"
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                mkdir build-artifacts
                                aws s3 cp ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/${PRESTO_PKG}     ./build-artifacts/ --no-progress
                                aws s3 cp ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/${PRESTO_CLI_JAR} ./build-artifacts/ --no-progress
                                docker buildx build -f docker/prestodb/Dockerfile --load --platform "${DOCKER_PLATFORM}" -t "${DOCKER_IMAGE}" -t "${DOCKER_IMAGE_LATEST}" \
                                    --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" .
                            '''
                        }
                    }
                }

                stage('Docker Native Build') {
                    steps {
                        echo "Building ${DOCKER_NATIVE_IMAGE}"
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''#!/bin/bash -ex
                                aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                docker buildx build -f Dockerfile-native --load --platform "${DOCKER_PLATFORM}" -t "${DOCKER_NATIVE_IMAGE}" -t "${DOCKER_NATIVE_IMAGE_LATEST}" \
                                    --build-arg "PRESTO_VERSION=${PRESTO_VERSION}" .
                                docker run -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY --entrypoint aws ${DOCKER_NATIVE_IMAGE} s3 cp /app/prestissimo.tar ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/ --no-progress
                            '''
                        }
                    }
                }

                stage('Publish Docker Images') {
                    when {
                        anyOf {
                            expression { params.PUBLISH_ARTIFACTS_ON_CURRENT_BRANCH }
                            branch "develop"
                        }
                        beforeAgent true
                    }

                    steps {
                        withCredentials([[
                                $class:            'AmazonWebServicesCredentialsBinding',
                                credentialsId:     "${AWS_CREDENTIAL_ID}",
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            sh '''
                                aws s3 ls ${AWS_S3_PREFIX}/${PRESTO_BUILD_VERSION}/
                                docker image ls
                                aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ECR}
                                docker push "${DOCKER_IMAGE}"
                                docker push "${DOCKER_NATIVE_IMAGE}"
                                docker buildx imagetools inspect "${DOCKER_IMAGE}"
                                docker buildx imagetools inspect "${DOCKER_NATIVE_IMAGE}"
                                docker push "${DOCKER_IMAGE_LATEST}"
                                docker push "${DOCKER_NATIVE_IMAGE_LATEST}"
                            '''
                        }
                    }
                }
            }
        }
    }

    post {
        fixed {
            slackSend(color: "good", message: "${RUN_DISPLAY_URL}")
        }
        failure {
            slackSend(color: "danger", message: "${RUN_DISPLAY_URL}")
        }
    }
}
