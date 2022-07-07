#!/usr/bin/env python
import boto3
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
    print(f"Called with stack {stackname}")
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
                and event["StackName"] != event["LogicalResourceId"]
                and event.get("ResourceStatusReason", "").lower()
                == "resource creation initiated"
            ):
                print(f'Found {event["PhysicalResourceId"]}')
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


def main(stackname: str, profile: str = "default", region: str = "us-east-2") -> None:
    first = True
    start = None
    data = {}
    ##
    events = retrieve_cf_events(stackname=stackname, profile=profile, region=region)
    events.sort(key=get_timestamp)
    # events.reverse()
    for event in events:
        if first:
            start = event["Timestamp"]
            first = False
        # Initialize the waterfall for this Resource
        if event["LogicalResourceId"] not in data:
            base = event["Timestamp"] - start
            data[event["LogicalResourceId"]] = {
                "result": {
                    "x": [], # x[0] is the group name, x[1] is the resource
                    "y": [], # y are the values for each resource
                    "text": [], # The text to the right of the bar, currently the total duration for that resource.
                    "textfont": {"family": "Open Sans, light", "color": "black"},
                    "textposition": "outside",
                    "width": 0.5,
                    "base": base.seconds,
                    "measure": [],
                    "increasing": {"marker": {"color": "LightBlue"}},
                    "legendgroup": event["StackName"],
                },
                "start_time": event["Timestamp"],
            }
        # Calculate the distance from stack start
        duration = (
            event["Timestamp"] - data[event["LogicalResourceId"]]["start_time"]
        )
        # If there are no values recorded for this resource ID, set the first
        # one as an absolute. TODO: this may need to change.
        if len(data[event["LogicalResourceId"]]["result"]["measure"]) == 0:
            data[event["LogicalResourceId"]]["result"]["measure"].append("absolute")
        # Otherwise set it as relative
        else:
            data[event["LogicalResourceId"]]["result"]["measure"].append("relative")
        # Record the results
        data[event["LogicalResourceId"]]["result"]["x"].append(duration.seconds)
        data[event["LogicalResourceId"]]["result"]["y"].append(
            event["LogicalResourceId"]
        )

        # If this is the last event then set a text label for the
        # total time the resource took to deploy.
        if event["ResourceStatus"] == "CREATE_COMPLETE":
            resource_duration = sum(data[event["LogicalResourceId"]]["result"]["x"])
            data[event["LogicalResourceId"]]["result"]["text"].append(
                format_time_from_seconds(resource_duration)
            )
        else:
            data[event["LogicalResourceId"]]["result"]["text"].append("")

    # Format the total run time for display
    total_time = format_time_from_seconds(data[stackname]["result"]["x"][-1])

    fig = go.Figure()
    fig.update_layout(
        title={
            "text": f'<span style="color:#000000">CloudFormation Waterfall - {stackname}<br /><b>Total Time: {total_time}</b></span>'
        },
        showlegend=False,
        height=(len(data) * 30),
        font={"family": "Open Sans, light", "color": "black", "size": 14},
        plot_bgcolor="#FFF",
        waterfallgroupgap = 0.5
    )
    fig.update_xaxes(
        title="Time in Seconds",
        tickangle=-45,
        tickfont=dict(family="Open Sans, light", color="black", size=12),
    )
    fig.update_yaxes(
        title="CloudFormation Resources",
        tickangle=0,
        tickfont=dict(family="Open Sans, light", color="black", size=12),
        linecolor="#000",
    )
    for k, v in data.items():
        fig.add_trace(go.Waterfall(orientation="h", **v["result"]))
    fig.show()


if __name__ == "__main__":
    fire.Fire(main)
