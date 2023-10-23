#!/usr/bin/env bash

function createBucket() {
    printf "Creating s3 storage: %s\n" $1
    awslocal s3 mb s3://$1

    printf "Setting CORS for s3 storage: %s\n" $1
    awslocal s3api put-bucket-cors --bucket $1 --cors-configuration file:///docker-entrypoint-initaws.d/cors-config.json
    awslocal s3api get-bucket-cors --bucket $1
}

# ... add more bucket names here

printf '\n\nListing all buckets created...'
awslocal s3 ls