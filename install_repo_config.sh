if [ "$artifactory_user_enabled" = true ]
then
    echo "============================ Working as artifactory user =================================="
    sh update_settings.sh
else
    echo "============================ Working as developer user =================================="
    sh installWKCDep.sh
fi
