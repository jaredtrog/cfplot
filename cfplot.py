#!/usr/bin/env python
import boto3
from collections import OrderedDict
import plotly.graph_objects as go
import fire


def format_time_from_seconds(seconds: int):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def get_timestamp(event: dict):
    return event["Timestamp"]


def retrieve_cf_events(stackname: str, profile: str, region: str) -> list:
    """
    Recursively retrieve all events including nested stacks
    """
    session = boto3.session.Session(profile_name=profile, region_name=region)
    cloudformation_client = session.client("cloudformation")
    paginator = cloudformation_client.get_paginator("describe_stack_events")
    pages_of_events = paginator.paginate(
        StackName=stackname, PaginationConfig={"MaxItems": 10000}
    )
    events = []
    for page in pages_of_events:
        for event in page["StackEvents"]:
            if (
                event["ResourceType"] == "AWS::CloudFormation::Stack"
                and event["StackName"] != event["PhysicalResourceId"]
                and event.get("ResourceStatusReason", "").lower()
                == "resource creation initiated"
            ):
                events.append(event)
                events.extend(
                    retrieve_cf_events(
                        stackname=event["PhysicalResourceId"],
                        profile=profile,
                        region=region,
                    )
                )
            else:
                events.append(event)
    return events

def construct_event_trace(start_time, data, event, is_total=False):
    trace = {
        "x": [],  # x are the values for each resource
        "y": [[], []],  # y[0] are the groups, y[1] are the names of each resource
        "text": [],  # The text to the right of the bar, currently the total duration for that resource.
        "textfont": {"family": "Open Sans, light", "color": "black"},
        "textposition": "outside",
        "width": 0.8,
        "base": (data["identified"] - start_time).seconds,
        "measure": [],
        "increasing": {"marker": {"color": "LightBlue"}},
        
    }
    # Identified
    trace["y"][0].append(event["StackName"])
    trace["y"][1].append(event["LogicalResourceId"])
    trace["x"].append(0)
    trace["measure"].append("relative")
    trace["text"].append("")
    if is_total:
        # total
        trace["y"][0].append(event["StackName"])
        trace["y"][1].append(event["LogicalResourceId"])
        trace["x"].append(None)
        trace["measure"].append("total")
        trace["text"].append(format_time_from_seconds(data["duration"].seconds))
    else:
        # Start
        trace["y"][0].append(event["StackName"])
        trace["y"][1].append(event["LogicalResourceId"])
        trace["x"].append(data["duration_i2s"].seconds)
        trace["measure"].append("relative")
        trace["text"].append("")
        # End
        trace["y"][0].append(event["StackName"])
        trace["y"][1].append(event["LogicalResourceId"])
        trace["x"].append(data["duration_s2e"].seconds)
        trace["measure"].append("relative")
        # Total duration
        trace["text"].append(format_time_from_seconds(data["duration"].seconds))
    return trace


def main(stackname: str, profile: str = "default", region: str = "us-east-2") -> None:
    data = OrderedDict()

    # Initialize the graph
    fig = go.Figure()

    ## Get the events in order
    events = retrieve_cf_events(stackname=stackname, profile=profile, region=region)
    events.sort(key=get_timestamp)
    start_time = events[0]["Timestamp"]
    ## Shape the events
    for event in events:
        stack_name = event["StackName"]
        logical_resource_id = event["LogicalResourceId"]
        resource_status = event["ResourceStatus"]
        resource_status_reason = event.get("ResourceStatusReason", "").lower()
        timestamp = event["Timestamp"]
        if stack_name not in data:
            data[stack_name] = {}
        # Assemble
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
            # Generate trace from recorded data for this logical resource
            data[stack_name][logical_resource_id].update({"end": timestamp})
            data[stack_name][logical_resource_id].update(
                {
                    "duration_i2s": data[stack_name][logical_resource_id]["start"]
                    - data[stack_name][logical_resource_id]["identified"]
                }
            )
            data[stack_name][logical_resource_id].update(
                {
                    "duration_s2e": timestamp
                    - data[stack_name][logical_resource_id]["start"]
                }
            )
            data[stack_name][logical_resource_id].update(
                {
                    "duration": timestamp
                    - data[stack_name][logical_resource_id]["identified"]
                }
            )
            # Generate trace and add.
            trace = construct_event_trace(
                start_time=start_time,
                data=data[stack_name][logical_resource_id],
                event=event
            )
            fig.add_trace(go.Waterfall(orientation="h", **trace))

    total_time = format_time_from_seconds(data[stackname][stackname]["duration"].seconds)
    # Display the figure
    fig.update_layout(
        title={
            "text": f'<span style="color:#000000">CloudFormation Waterfall - {stackname}<br /><b>Total Time: {total_time}</b></span>'
        },
        showlegend=False,
        height=(len(events) * 10),
        font={"family": "Open Sans, light", "color": "black", "size": 14},
        plot_bgcolor="#FFF",
    )
    fig.update_xaxes(
        title="Time in Seconds",
        tickangle=-45,
        tickfont=dict(family="Open Sans, light", color="black", size=14),
    )
    fig.update_yaxes(
        title="CloudFormation Resources",
        tickangle=0,
        tickfont=dict(family="Open Sans, light", color="black", size=14),
        linecolor="#000",
    )
    fig.update_traces(connector_visible=False)
    fig.show()


if __name__ == "__main__":
    fire.Fire(main)
