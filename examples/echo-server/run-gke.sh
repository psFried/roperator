#!/bin/bash

# This script makes it easier to run the echo-server example against a GKE cluster using a GCP Service Account
# If you're using GKE, then check out the GKE Dev Authentication help page at:
# https://psfried.github.io/roperator/reference/gke-dev-auth.html
# If you're not using GKE, then you can just ignore this script and use `cargo run --example echo-server`
#
# To use this, you need to first follow the instructions in the above guide in order to create a service
# account and download a key file. Once you have the key file downloaded, run this script, with the path
# to the key file as an argument. For example:
# $ ./examples/echo-server/run-gke.sh path/to/keyfile.json

set -e

if [[ -f "$1" ]]; then
    export GOOGLE_APPLICATION_CREDENTIALS="$1"
    export ROPERATOR_AUTH_TOKEN=$(gcloud auth application-default print-access-token)
    cargo run --example echo-server
else
    echo "provide the path to your GCP Service Account key file as an argument"
    echo "example: ./examples/echo-server/run-gke.sh path/to/keyfile.json"
fi

