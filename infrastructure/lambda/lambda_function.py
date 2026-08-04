{
  "Name": "marketpulse-start-airflow-ec2",
  "ScheduleExpression": "cron(55 23 * * ? *)",
  "ScheduleExpressionTimezone": "America/Sao_Paulo",
  "FlexibleTimeWindow": {
    "Mode": "OFF"
  },
  "State": "DISABLED",
  "Target": {
    "Arn": "arn:aws:lambda:sa-east-1:966725470611:function:marketpulse-start-ec2",
    "RoleArn": "arn:aws:iam::966725470611:role/marketpulse-start-airflow-scheduler-role",
    "Input": "{}",
    "RetryPolicy": {
      "MaximumRetryAttempts": 2
    }
  }
}
"""Start the MarketPulse EC2 instance from EventBridge Scheduler."""

import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
AWS_REGION = "sa-east-1"


def lambda_handler(event, context):
    """Start the configured instance if it is stopped; never wait for boot."""
    del event, context
    instance_id = os.environ.get("EC2_INSTANCE_ID")
    if not instance_id:
        LOGGER.error("EC2_INSTANCE_ID environment variable is not configured.")
        raise ValueError("EC2_INSTANCE_ID environment variable is required")

    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        reservations = response.get("Reservations", [])
        instances = reservations[0].get("Instances", []) if reservations else []
        if not instances:
            raise RuntimeError(f"Instance {instance_id} was not returned by EC2")
        previous_state = instances[0]["State"]["Name"]
        LOGGER.info("Instance %s current state: %s", instance_id, previous_state)

        if previous_state == "stopped":
            ec2.start_instances(InstanceIds=[instance_id])
            action = "start_requested"
            LOGGER.info("Start requested for instance %s.", instance_id)
        elif previous_state in {"pending", "running"}:
            action = "no_action_already_starting_or_running"
            LOGGER.info("No start requested for instance %s.", instance_id)
        else:
            action = "no_action_instance_not_startable"
            LOGGER.warning("Instance %s is not startable: %s", instance_id, previous_state)

        return {
            "instance_id": instance_id,
            "previous_state": previous_state,
            "action": action,
        }
    except (ClientError, BotoCoreError, RuntimeError):
        LOGGER.exception("Failed to process instance %s.", instance_id)
        raise
