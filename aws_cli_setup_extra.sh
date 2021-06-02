#!/bin/bash

aws configure --profile default 
aws configure set aws_access_key_id 
aws configure set aws_secret_access_key 
aws configure set region us-east-2

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION=us-east-2
