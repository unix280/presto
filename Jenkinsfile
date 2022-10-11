pipeline {

    agent {
        kubernetes {
            defaultContainer 'maven'
            yamlFile 'jenkins/agent-maven.yaml'
        }
    }

    environment {
        ECR_URL = "299095523242.dkr.ecr.us-east-1.amazonaws.com"
        AWS_DEFAULT_REGION = "us-east-1"
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '100'))
        timeout(time: 60, unit: 'MINUTES')
    }

    parameters {
        booleanParam(name: 'PUBLISH_IMAGE_ON_CURRENT_BRANCH',
                     defaultValue: false,
                     description: 'whether to build docker image on current branch/commit, even it is not master')
        string(name: 'AWS_CREDENTIAL_ID',
               defaultValue: 'aws-jenkins')
    }

    stages {
        stage('Maven S3 Credential') {
            steps {
                withCredentials([[
                        $class: 'AmazonWebServicesCredentialsBinding',
                        credentialsId: 'maven-s3-private-repo',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                    sh '''
                        sed -i "s#MAVEN_S3_ACCESS_KEY#$AWS_ACCESS_KEY_ID#g" settings.xml
                        sed -i "s#MAVEN_S3_SECRET_KEY#$AWS_SECRET_ACCESS_KEY#g" settings.xml
                        cat settings.xml
                    '''
                }
            }
        }

        stage('Maven Build') {
            steps {
                script {
                    env.PRESTO_VERSION = sh(script: 'bash jenkins/get-presto-version.sh', returnStdout: true).trim()
                    env.PRESTO_PKG = "presto-server-${PRESTO_VERSION}.tar.gz"
                    env.PRESTO_CLI_JAR = "presto-cli-${PRESTO_VERSION}-executable.jar"
                    env.BUILD_VERSION = sh(script: 'bash jenkins/get-build-version.sh', returnStdout: true).trim()
                }
                sh 'printenv | sort'

                echo 'build prestodb source code with build version ${BUILD_VERSION}'
                sh '''
                    export MAVEN_CONFIG=''
                    ./mvnw -v
                    ./mvnw install -DskipTests -B -T C1 -P ci -pl '!presto-docs'
                '''
            }
        }

        stage('Package') {
            steps {
                echo 'create docker build context package'
                sh '''
                    mkdir -p "prestodb-${BUILD_VERSION}/build-artifacts"
                    cp -r docker "prestodb-${BUILD_VERSION}/"
                    cp presto-server/target/${PRESTO_PKG} "prestodb-${BUILD_VERSION}/build-artifacts/"
                    cp presto-cli/target/${PRESTO_CLI_JAR} "prestodb-${BUILD_VERSION}/build-artifacts/"
                    tar -C "prestodb-${BUILD_VERSION}" -czvf "prestodb-${BUILD_VERSION}.tar.gz" .
                '''
            }
        }

        stage('Docker Build') {
            when {
                anyOf {
                    expression { params.PUBLISH_IMAGE_ON_CURRENT_BRANCH }
                    branch "develop"
                }
                beforeAgent true
            }
            steps {
                withCredentials([[
                        $class: 'AmazonWebServicesCredentialsBinding',
                        credentialsId: "${AWS_CREDENTIAL_ID}",
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                    sh '''
                        echo "upload prestodb-${BUILD_VERSION}.tar.gz to S3"
                        aws s3 cp --no-progress prestodb-${BUILD_VERSION}.tar.gz s3://ahana-jenkins/kaniko-image-context/prestodb/

                        echo 'build prestodb docker image'
                        envsubst < jenkins/pod-kaniko.yaml | kubectl apply -f -
                    '''
                }
                sh '''
                    sleep 60
                    POD_NAME="kaniko-${BUILD_VERSION}-${BUILD_NUMBER}"
                    set -x
                    kubectl wait --for=condition=Ready pod/${POD_NAME} --timeout=600s
                    kubectl logs -f ${POD_NAME} 2>&1 | tee kaniko-build.log
                    cat kaniko-build.log | grep "Pushed 299095523242.dkr.ecr.us-east-1.amazonaws.com/ahanaio/prestodb"
                    kubectl delete pod/${POD_NAME}
                    kubectl wait --for=delete pod/${POD_NAME} --timeout=60s
                '''
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
