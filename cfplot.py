#!/usr/bin/env python
import boto3
import plotly.graph_objects as go
import fire

def format_time_from_seconds(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours:02}:{minutes:02}:{seconds:02}'

def order_events(events):
    # Reverse the order of pages
    for e in events:
        e['StackEvents'].reverse()
    events.reverse()
    return events

def main(stackname, profile='default', region='us-east-2'):
    session = boto3.session.Session(
        profile_name=profile, region_name=region)
    cloudformation_client = session.client('cloudformation')
    paginator = cloudformation_client.get_paginator('describe_stack_events')
    response = paginator.paginate(
        StackName=stackname)
    first = True
    start = None
    data = {}
    # Drop it, flip it, and reverse it.
    events = order_events([e for e in response])
    for page in events:
        for event in page['StackEvents']:
            if first:
                start = event['Timestamp']
                first = False
            # Initialize the waterfall for this Resource
            if event['LogicalResourceId'] not in data:
                base = event['Timestamp'] - start
                data[event['LogicalResourceId']] = {
                    'result': {
                        'x': [],
                        'y': [],
                        'text': [],
                        'textfont': {"family": "Open Sans, light",
                                    "color": "black"
                                    },
                        'textposition': "outside",
                        'width': 0.5,
                        'base': base.seconds,
                        'measure': [],
                        'increasing': {"marker": {"color": "Teal"}}},
                    'start_time': event['Timestamp']}
            # Calculate the distance from stack start
            duration = event['Timestamp'] - data[event['LogicalResourceId']]['start_time']
            # If there are no values recorded for this resource ID, set the first
            # one as an absolute. TODO: this may need to change.
            if len(data[event['LogicalResourceId']]['result']['measure']) == 0:
                data[event['LogicalResourceId']
                    ]['result']['measure'].append('absolute')
            # Otherwise set it as relative
            else:
                data[event['LogicalResourceId']
                    ]['result']['measure'].append('relative')
            # Record the results
            data[event['LogicalResourceId']]['result']['x'].append(
                duration.seconds)
            data[event['LogicalResourceId']]['result']['y'].append(
                event['LogicalResourceId'])
            
            # If this is the last event then set a text label for the
            # total time the resource took to deploy.
            if event['ResourceStatus'] == 'CREATE_COMPLETE':
                resource_duration = sum(data[event['LogicalResourceId']]['result']['x'])
                data[event['LogicalResourceId']]['result']['text'].append(
                    format_time_from_seconds(resource_duration))
            else:
                data[event['LogicalResourceId']]['result']['text'].append(
                    '')

    # Format the total run time for display
    total_time = format_time_from_seconds(data[stackname]["result"]["x"][-1])
    
    fig = go.Figure()
    fig.update_layout(title={
        'text': f'<span style="color:#000000">CloudFormation Waterfall - {stackname}<br /><b>Total Time: {total_time}</b></span>'
    },
        showlegend=False,
        height=(len(data)*30),
        font={
        'family': 'Open Sans, light',
        'color': 'black',
        'size': 14
    },
        plot_bgcolor='rgba(0,0,0,0)')
    fig.update_xaxes(
        tickangle=-45, tickfont=dict(family='Open Sans, light', color='black', size=12))
    fig.update_yaxes(tickangle=0, tickfont=dict(
        family='Open Sans, light', color='black', size=12))
    for k, v in data.items():
        fig.add_trace(go.Waterfall(orientation='h', **v['result']))
    fig.show()


if __name__ == '__main__':
    fire.Fire(main)
