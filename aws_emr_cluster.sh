#!/bin/bash

aws emr create-cluster --name emr-dl-cluster \
 --use-default-roles --release-label emr-5.30.0  \
--instance-count 3 --applications Name=Spark Name=Zeppelin  \
--ec2-attributes KeyName="my-emr-cluster" \
--instance-type m5.xlarge --log-uri s3://course-datalake-prj/emr-logs/ \
--instance-count 3
