#!/bin/bash

# Script for updating seastar submodule.
# Contains a check whether all changes are fast-forward.
# Example:
# git fetch
# git checkout origin/next
# ./scripts/update_seastar_submodule.sh
# git push origin HEAD:refs/heads/next

set -e

cd seastar >& /dev/null || cd ../seastar >& /dev/null || ( echo Please run the script from the root directory or scripts/ && exit 1 )

git fetch
git checkout origin/master
cd ..

SEASTAR_SUMMARY=$(git submodule summary seastar)

if grep '^ *<' <<< "$SEASTAR_SUMMARY"; then
    echo "Non fast-forward changes detected! Fire three red flares from your flare pistol."
    exit 1
fi
if [ "$SEASTAR_SUMMARY" == "" ]; then
    echo "Everything up-to-date!"
    exit 0
fi

git commit seastar -m "Update seastar submodule" -m "$SEASTAR_SUMMARY"
git commit --amend # for a manual double-check

