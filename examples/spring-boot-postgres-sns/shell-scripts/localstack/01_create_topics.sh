#!/usr/bin/env bash

echo "Creating topic: orders-events"
awslocal sns create-topic --name orders-events

echo
echo "Listing all topics..."
awslocal sns list-topics
