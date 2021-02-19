# This script rotates IAM keys for the current user

circleCIOwnerId=$1
projectName=$2
environment=$3
iamUserName=$4

# We store the current key for deletion, as it will change throughout this process
oldKey=$(aws iam list-access-keys --user-name ${iamUserName} | jq '.AccessKeyMetadata | .[].AccessKeyId' | tr -d '"')
echo Found $oldKey for $iamUserName

# Create new access key
accessKey=$(aws iam create-access-key --user-name ${iamUserName} --region us-east-1 | jq .AccessKey)
newAccessKeyId=$(echo $accessKey | jq .AccessKeyId)
newSecretAccessKey=$(echo $accessKey | jq .SecretAccessKey)
echo Created new access key $newAccessKeyId for $iamUserName

# Fetch and traverse all contexts to get relevant context's ID dynamically
allContexts=$(curl --request GET --url https://circleci.com/api/v2/context?owner-id=${circleCIOwnerId} --header "Circle-Token: ${CIRCLECI_API_KEY}")
allContexts=$(echo $allContexts | jq .items )

for row in $(echo "${allContexts}" | jq -r '.[] | @base64'); do
    _jq() {
      echo ${row} | base64 --decode | jq -r ${1}
    }

contextName=$(_jq '.name')

# If the current context from the array matches the corresponding CircleCI provisioning environment,
# we keep track of its ID to use it when updating the env variables via API
if [[ $contextName == "${projectName}_${environment}" ]]; then
    contextId=$(_jq '.id')
fi
done
echo Found CircleCi context ${projectName}_${environment} $contextId

# Update the access key and secret access key CircleCI environment variables using cURL
curl --request PUT \
--url https://circleci.com/api/v2/context/${contextId}/environment-variable/AWS_ACCESS_KEY_ID_TEST \
--header "Circle-Token: ${CIRCLECI_API_KEY}" \
--header 'Accept: application/json'    \
--header 'Content-Type: application/json' \
--data "{\"value\":$newAccessKeyId}"

echo
echo Updated ${projectName}_${environment} AWS_ACCESS_KEY_ID to $newAccessKeyId

curl --request PUT \
--url https://circleci.com/api/v2/context/${contextId}/environment-variable/AWS_SECRET_ACCESS_KEY_TEST \
--header "Circle-Token: ${CIRCLECI_API_KEY}" \
--header 'Accept: application/json'    \
--header 'Content-Type: application/json' \
--data "{\"value\":$newSecretAccessKey}"

echo
echo Updated ${projectName}_${environment} AWS_SECRET_ACCESS_KEY

# Delete previous key from AWS
aws iam delete-access-key --access-key-id $oldKey --user-name ${iamUserName} --region us-east-1
echo Deleted $oldKey from $iamUserName
