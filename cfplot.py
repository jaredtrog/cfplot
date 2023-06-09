#!/usr/bin/env python
import boto3
import plotly.graph_objects as go
from collections import OrderedDict
import fire
import logging

# Constants
SECONDS_IN_HOUR = 3600
SECONDS_IN_MINUTE = 60
MAX_ITEMS = 10000
DEFAULT_PROFILE = "default"
DEFAULT_REGION = "us-east-2"
DEFAULT_FONT = {"family": "Open Sans, light", "color": "black", "size": 14}

logging.basicConfig(level=logging.INFO)


def format_time_from_seconds(seconds: int) -> str:
    hours, remainder = divmod(seconds, SECONDS_IN_HOUR)
    minutes, seconds = divmod(remainder, SECONDS_IN_MINUTE)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def get_timestamp(event: dict) -> str:
    return event["Timestamp"]


def retrieve_cf_events(stackname: str, profile: str, region: str) -> list:
    """
    Recursively retrieve all events including nested stacks
    """
    session = boto3.session.Session(profile_name=profile, region_name=region)
    cloudformation_client = session.client("cloudformation")
    paginator = cloudformation_client.get_paginator("describe_stack_events")
    pages_of_events = paginator.paginate(
        StackName=stackname, PaginationConfig={"MaxItems": MAX_ITEMS}
    )
    events = []
    for page in pages_of_events:
        for event in page["StackEvents"]:
            events.append(event)
            if (
                event["ResourceType"] == "AWS::CloudFormation::Stack"
                and event["StackName"] != event["PhysicalResourceId"]
                and event.get("ResourceStatusReason", "").lower()
                == "resource creation initiated"
            ):
                events.extend(
                    retrieve_cf_events(
                        stackname=event["PhysicalResourceId"],
                        profile=profile,
                        region=region,
                    )
                )
    return events


def construct_event_trace(start_time, data, event, is_total=False):
    trace = {
        "x": [],
        "y": [[], []],
        "text": [],
        "textfont": DEFAULT_FONT,
        "textposition": "outside",
        "width": 0.8,
        "base": (data["identified"] - start_time).seconds,
        "measure": [],
        "increasing": {"marker": {"color": "LightBlue"}},
    }
    update_trace(event, trace, is_total, data)
    return trace


def update_trace(event, trace, is_total, data):
    trace["y"][0].append(event["StackName"])
    trace["y"][1].append(event["LogicalResourceId"])
    trace["x"].append(0 if is_total else data["duration_i2s"].seconds)
    trace["measure"].append("relative")
    trace["text"].append("")
    if is_total:
        trace["text"].append(format_time_from_seconds(data["duration"].seconds))
    else:
        trace["y"][0].append(event["StackName"])
        trace["y"][1].append(event["LogicalResourceId"])
        trace["x"].append(data["duration_s2e"].seconds)
        trace["measure"].append("relative")
        trace["text"].append(format_time_from_seconds(data["duration"].seconds))


def main(stackname: str, profile: str = DEFAULT_PROFILE, region: str = DEFAULT_REGION) -> None:
    logging.info(f"Starting retrieval of events for stack: {stackname}")
    data = OrderedDict()
    fig = go.Figure()
    try:
        events = retrieve_cf_events(stackname=stackname, profile=profile, region=region)
        events.sort(key=get_timestamp)
        start_time = events[0]["Timestamp"]
        process_events(events, start_time, data, fig)
        display_figure(fig, data, events, stackname)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


def process_events(events, start_time, data, fig):
    for event in events:
        update_data_for_event(event, data)
        if event["ResourceStatus"] == "CREATE_COMPLETE":
            trace = construct_event_trace(
                start_time=start_time,
                data=data[event["StackName"]][event["LogicalResourceId"]],
                event=event
            )
            fig.add_trace(go.Waterfall(orientation="h", **trace))


def update_data_for_event(event, data):
    stack_name = event["StackName"]
    logical_resource_id = event["LogicalResourceId"]
    resource_status = event["ResourceStatus"]
    resource_status_reason = event.get("ResourceStatusReason", "").lower()
    timestamp = event["Timestamp"]
    if stack_name not in data:
        data[stack_name] = {}
    if (
        stack_name == logical_resource_id
        and resource_status_reason == "user initiated"
    ):
        data[stack_name][logical_resource_id] = {
            "identified": timestamp,
            "start": timestamp,
        }
    elif logical_resource_id not in data[stack_name]:
        data[stack_name][logical_resource_id] = {"identified": timestamp}
    elif (
        resource_status == "CREATE_IN_PROGRESS"
        and resource_status_reason == "resource creation initiated"
    ):
        data[stack_name][logical_resource_id].update({"start": timestamp})
    elif resource_status == "CREATE_IN_PROGRESS" and resource_status_reason == "":
        data[stack_name][logical_resource_id].update({"identified": timestamp})
    elif resource_status == "CREATE_COMPLETE":
        data[stack_name][logical_resource_id].update(
            {
                "end": timestamp,
                "duration_i2s": data[stack_name][logical_resource_id]["start"]
                - data[stack_name][logical_resource_id]["identified"],
                "duration_s2e": timestamp
                - data[stack_name][logical_resource_id]["start"],
                "duration": timestamp
                - data[stack_name][logical_resource_id]["identified"],
            }
        )


def display_figure(fig, data, events, stackname):
    total_time = format_time_from_seconds(data[stackname][stackname]["duration"].seconds)
    fig.update_layout(
        title={
            "text": f'<span style="color:#000000">CloudFormation Waterfall - {stackname}<br /><b>Total Time: {total_time}</b></span>'
        },
        showlegend=False,
        height=(len(events) * 10),
        font=DEFAULT_FONT,
        plot_bgcolor="#FFF",
    )
    fig.update_xaxes(
        title="Event Duration",
        tickangle=-45,
        tickfont=DEFAULT_FONT,
    )
    fig.update_yaxes(
        title="CloudFormation Resources",
        tickangle=0,
        tickfont=DEFAULT_FONT,
        linecolor="#000",
    )
    fig.update_traces(connector_visible=False)
    fig.show()


if __name__ == "__main__":
    fire.Fire(main)
