#!/usr/bin/env bash
aws lambda invoke \
    --invocation-type RequestResponse \
    --function-name synergy-dispatcher \
    --region eu-west-1 \
    --log-type Tail \
    --cli-binary-format raw-in-base64-out \
    --payload '{"eventID":"556e4567-e89d-12d3-a456-426655440007","eventAction":"doSomething","eventData" : {"data1" : "hello","data2" : "world"}}' \
    outfile.txt