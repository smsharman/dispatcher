#!/bin/bash
aws lambda create-function \
    --region eu-west-1 \
    --function-name synergy-dispatcher \
    --zip-file fileb://$(pwd)/../target/synergy-dispatcher.jar \
    --role arn:aws:iam::979590819078:role/syn-evt-lambda \
    --handler dispatcher.core.Route \
    --runtime java11 \
    --timeout 15 \
    --memory-size 1024 \
    --cli-connect-timeout 6000