#!/bin/bash

if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 <github_user> <github_access_token>"
    exit 1
fi

curl -L -o /tmp/presto_release "https://m2.unidevel.cn/releases/com/facebook/presto/presto-release-tools/0.9-SNAPSHOT/presto-release-tools-0.9-20250310.110830-1-executable.jar"
chmod 755 /tmp/presto_release
/tmp/presto_release release-notes --github-user $1 --github-access-token $2
