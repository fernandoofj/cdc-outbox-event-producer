#!/usr/bin/env bash

function createQueue() {
    printf "Creating queue: %s\n" $1
    awslocal sqs create-queue --queue-name $1
}

# ... add more queue names here

printf '\n\nListing all queues created...'

awslocal sqs list-queues
