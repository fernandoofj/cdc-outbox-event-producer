#!/usr/bin/env bash

# Makes the sample self-verifying: subscribes an SQS queue to the
# orders-events topic so `awslocal sqs receive-message` (see the
# sample's README § "Check it landed") can show the published event
# without needing to wire a real consumer.

echo "Creating queue: orders-events-check"
awslocal sqs create-queue --queue-name orders-events-check

echo "Subscribing orders-events-check to orders-events"
awslocal sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:orders-events \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:orders-events-check

echo
echo "Listing subscriptions..."
awslocal sns list-subscriptions
