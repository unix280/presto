######################################################################################
##
## This is meant for local development builds to push to ECR repos in developer
## AWS accounts. Do not use this for deployment to production or staging!!!!
## This will build and push docker images to the repositories set up using
## the machineray terraform. Follow the 'Deploy MachineRay Terraform' steps in that
## repository's README. Use the same AWS access credentials in the .env for
## this repository
##
######################################################################################

set -e

# Install Maven Wrapper
./mvnw -v

mvn_version=$(./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout)
base_os='centos:7'

# Run Maven Build
./mvnw install -DskipTests -B -T C1 -P ci

# Copy build artifacts
rm -f build-artifacts
mkdir build-artifacts
cp -n presto-server/target/presto-server-*.tar.gz ./build-artifacts
cp -n presto-server-rpm/target/presto-server-rpm-*.x86_64.rpm ./build-artifacts
cp -n presto-product-tests/target/presto-product-tests-*-executable.jar ./build-artifacts
cp -n presto-jdbc/target/presto-jdbc-*.jar ./build-artifacts
cp -n presto-cli/target/presto-cli-*-executable.jar ./build-artifacts

# Get ECR repository information
prestodb_repo_url=$(aws ecr describe-repositories --repository-names "ahanaio/prestodb" | grep -Eo '\d{12}\.dkr\.ecr\..*\.amazonaws.com/ahanaio/prestodb')
echo $prestodb_repo_url
prestodb_cli_repo_url=$(aws ecr describe-repositories --repository-names "ahanaio/prestodb-cli" | grep -Eo '\d{12}\.dkr\.ecr\..*\.amazonaws.com/ahanaio/prestodb-cli')
echo $prestodb_cli_repo_url
account_url=$(echo "$prestodb_repo_url" | grep -Eo '\d{12}\.dkr\.ecr\..*\.amazonaws.com')
echo $account_url

# Build prestodb docker image
docker build -f docker/prestodb/Dockerfile -t ahanaio/prestodb:latest --build-arg PRESTO_VERSION=$mvn_version --build-arg BASE_OS=$base_os .

# Tag prestodb docker image
aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $account_url
docker tag ahanaio/prestodb:latest $account_url/ahanaio/prestodb:latest
docker tag ahanaio/prestodb:latest $account_url/ahanaio/prestodb:$mvn_version

# Push prestodb docker image
docker push $account_url/ahanaio/prestodb:latest
docker push $account_url/ahanaio/prestodb:$mvn_version


# Build prestodb-cli docker image
docker build -f docker/prestodb-cli/Dockerfile -t ahanaio/prestodb-cli:latest --build-arg PRESTO_VERSION=$mvn_version --build-arg BASE_OS=$base_os .

# Tag prestodb-cli docker image
docker tag ahanaio/prestodb-cli:latest $account_url/ahanaio/prestodb-cli:latest
docker tag ahanaio/prestodb-cli:latest $account_url/ahanaio/prestodb-cli:$mvn_version

# Push prestodb-ci docker image
docker push $account_url/ahanaio/prestodb-cli:latest
docker push $account_url/ahanaio/prestodb-cli:$mvn_version
