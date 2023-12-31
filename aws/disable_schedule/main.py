import boto3
import os

scheduler = boto3.client('scheduler')
schedule_name = os.environ['ScheduleName']
state = os.environ['State']


def handler(event, context):
    response = scheduler.get_schedule(
        Name=schedule_name
    )
    sqs_templated = {
        "RoleArn": response['Target']['RoleArn'],
        "Arn": response['Target']['Arn'],
        "Input": "{}"}

    flex_window = { "Mode": "OFF" }

    scheduler.update_schedule(Name=schedule_name,
                              ScheduleExpression=response['ScheduleExpression'],
                              Target=sqs_templated,
                              FlexibleTimeWindow=flex_window,
                              State=state)