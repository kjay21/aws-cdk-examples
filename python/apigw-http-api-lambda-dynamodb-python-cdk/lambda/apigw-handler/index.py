# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from datetime import datetime
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all AWS SDK calls for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


@xray_recorder.capture('lambda_handler')
def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Log request context for security analysis
    logger.info(json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "request_id": context.aws_request_id,
        "source_ip": event.get("requestContext", {}).get("identity", {}).get("sourceIp"),
        "user_agent": event.get("requestContext", {}).get("identity", {}).get("userAgent"),
        "table_name": table,
        "http_method": event.get("httpMethod"),
        "resource_path": event.get("resource"),
        "action": "request_received"
    }))
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            
            # Log data validation
            logger.info(json.dumps({
                "action": "data_validation",
                "request_id": context.aws_request_id,
                "payload_received": True,
                "item_keys": list(item.keys()) if isinstance(item, dict) else "invalid_format"
            }))
            
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            
            # Log successful operation
            logger.info(json.dumps({
                "action": "dynamodb_put_item",
                "request_id": context.aws_request_id,
                "status": "success",
                "item_id": id
            }))
            
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            # Log default data insertion
            logger.info(json.dumps({
                "action": "default_data_insertion",
                "request_id": context.aws_request_id,
                "payload_received": False
            }))
            
            item_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": item_id},
                },
            )
            
            logger.info(json.dumps({
                "action": "dynamodb_put_item",
                "request_id": context.aws_request_id,
                "status": "success",
                "item_id": item_id
            }))
            
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
            
    except Exception as e:
        # Log errors for security analysis
        logger.error(json.dumps({
            "action": "error",
            "request_id": context.aws_request_id,
            "error_type": type(e).__name__,
            "error_message": str(e)
        }))
        
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Internal server error"}),
        }