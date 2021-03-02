# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from __future__ import print_function
import boto3
import json
import random
import string
import urllib3

http = urllib3.PoolManager()

SUCCESS = "SUCCESS"
FAILED = "FAILED"
SERVER_PROPERTIES = b"""
auto.create.topics.enable=true
default.replication.factor=2
        """


def lambda_handler(event, context):
    kafka = boto3.client("kafka")
    physical_id = "None"
    random_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    revision = 1
    if event["RequestType"] == "Create":
        config = kafka.create_configuration(Name=event["LogicalResourceId"] + "-" + random_id,
                                            ServerProperties=SERVER_PROPERTIES)
        physical_id = config["Arn"]
        revision = config["LatestRevision"]["Revision"]
    elif event["RequestType"] == "Delete":
        kafka.delete_configuration(Arn=event["PhysicalResourceId"])

    send(event, context, SUCCESS, {
        "Revision": revision,
        "Arn": physical_id
    }, physical_id)


def send(event, context, response_status, response_data, physical_resource_id=None):
    response_url = event['ResponseURL']
    response_body = {
        'Status': response_status,
        'Reason': "See the details in CloudWatch Log Stream: {}".format(context.log_stream_name),
        'PhysicalResourceId': physical_resource_id,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'NoEcho': False,
        'Data': response_data
    }

    json_response_body = json.dumps(response_body)


    headers = {
        'content-type': '',
        'content-length': str(len(json_response_body))
    }

    try:
        response = http.request('PUT', response_url, headers=headers, body=json_response_body)
        print("Status code:", response.status)

    except Exception as e:
        print("send(..) failed executing http.request(..):", e)
