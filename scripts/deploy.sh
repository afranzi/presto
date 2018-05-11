#!/usr/bin/env bash

set -e

# This script will deploy project packages to artifactory.
# https://confluence.schibsted.io/display/SPTINF/Bash+helpers+methods+available+in+the+Travis+builds
source /usr/local/share/bash/travis_dependencies.bash

BASE_PATH=spt/data/sqlaas/presto
PACKAGE="/tmp/artifacts/presto-server-*.tar.gz"

deploy_ephemeral() {
    jfrog rt upload ${PACKAGE} generic-ephemeral/${BASE_PATH}/${TRAVIS_BRANCH}/${TRAVIS_BUILD_NUMBER}/
}

deploy_tag() {
    jfrog rt upload ${PACKAGE} generic-local/${BASE_PATH}/release/${TRAVIS_TAG}/
}

main() {
    configure_jfrog_client

    if [ "$TRAVIS_TAG" ]; then
        deploy_tag
    else
        deploy_ephemeral
    fi
}

main


