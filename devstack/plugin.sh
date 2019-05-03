#!/bin/bash

if [[ "$1" == "stack" && "$2" == "install" ]]; then

    echo "Cherry-picking latest driver from gerrit"

    cd /opt/stack/cinder

    # fetch latest patchset from gerrit
    UPSTREAM_REMOTE=https://review.opendev.org/openstack/cinder

    # set this to your gerrit change number
    CHANGE_NUM=617503

    PATCHSET_BASE=refs/changes/${CHANGE_NUM:(-2)}/$CHANGE_NUM
    LATEST_PATCHSET=$(git ls-remote $UPSTREAM_REMOTE $PATCHSET_BASE/\* |
                      sort -t/ -k 5 -n | tail -n1 | cut -d$'\t' -f2)

    if [ -z "$LATEST_PATCHSET" ]; then
        echo "Failed to determine latest patchset of $PATCHSET_BASE from $UPSTREAM_REMOTE"
    fi
     
    echo "Latest patchset ref is $LATEST_PATCHSET"
    touch /home/tempest/devstack/commit-id
    echo 'cinder_commit_id ' | tr -d '\n'  > /home/tempest/devstack/commit-id && git rev-parse --short HEAD >> /home/tempest/devstack/commit-id

    cd /opt/stack/quantastor
    git clone $UPSTREAM_REMOTE
    cd /opt/stack/quantastor/cinder
    git fetch $UPSTREAM_REMOTE $LATEST_PATCHSET && git checkout FETCH_HEAD
    if [ ! -e /opt/stack/cinder/cinder/volume/drivers/osnexus/quantastor.py ]; then
	cp /opt/stack/quantastor/cinder/cinder/volume/drivers/osnexus/quantastor.py /opt/stack/cinder/cinder/volume/drivers/osnexus/quantastor.py
	cp /opt/stack/quantastor/cinder/cinder/volume/drivers/osnexus/quantastor_api.py /opt/stack/cinder/cinder/volume/drivers/osnexus/quantastor_api.py
    fi
fi

