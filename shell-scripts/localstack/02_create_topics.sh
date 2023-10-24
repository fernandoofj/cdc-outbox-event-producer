#!/usr/bin/env bash

function createTopic() {
    printf "Creating topic: %s\n" $1
    awslocal sns create-topic --name $1
}

createTopic test-business-events
# ... add more topic names here

printf '\n\nListing all topics created...'

awslocal sns list-topics
