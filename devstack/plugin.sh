#!/bin/bash

if [[ "$1" == "stack" && "$2" == "install" ]]; then

    echo "Cherry-picking latest driver from gerrit"

    cd /opt/stack/cinder

    # fetch latest patchset from gerrit
    UPSTREAM_REMOTE=https://git.openstack.org/openstack/cinder

    # set this to your gerrit change number
    CHANGE_NUM=617503

    PATCHSET_BASE=refs/changes/${CHANGE_NUM:(-2)}/$CHANGE_NUM
    LATEST_PATCHSET=$(git ls-remote $UPSTREAM_REMOTE $PATCHSET_BASE/\* |
                      sort -t/ -k 5 -n | tail -n1 | cut -d$'\t' -f2)

    if [ -z "$LATEST_PATCHSET" ]; then
        echo "Failed to determine latest patchset of $PATCHSET_BASE from $UPSTREAM_REMOTE"
        exit 1
    fi

    echo "Latest patchset ref is $LATEST_PATCHSET"

    git fetch $UPSTREAM_REMOTE $LATEST_PATCHSET && git cherry-pick --continue FETCH_HEAD



fi

