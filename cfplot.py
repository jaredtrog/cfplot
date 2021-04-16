#!/usr/bin/env python
from inspect import stack
import boto3
import plotly.graph_objects as go
import fire


def main(stackname, profile='default', region='us-east-2'):
    session = boto3.session.Session(
        profile_name=profile, region_name=region)
    cloudformation_client = session.client('cloudformation')
    response = cloudformation_client.describe_stack_events(
        StackName=stackname)
    first = True
    start = None
    data = {}
    # Reverse the order to get events oldest to newest
    response['StackEvents'].reverse()
    for r in response['StackEvents']:
        if first:
            start = r['Timestamp']
            first = False
        if r['LogicalResourceId'] not in data:
            base = r['Timestamp'] - start
            data[r['LogicalResourceId']] = {
                'result': {
                    'x': [],
                    'y': [[], []],
                    'base': base.seconds,
                    'measure': []},
                'start_time': r['Timestamp']}
        duration = r['Timestamp'] - data[r['LogicalResourceId']]['start_time']
        if len(data[r['LogicalResourceId']]['result']['measure']) == 0:
            data[r['LogicalResourceId']
                 ]['result']['measure'].append('absolute')
        else:
            data[r['LogicalResourceId']
                 ]['result']['measure'].append('relative')
            data[r['LogicalResourceId']]['result']['x'].append(
                duration.seconds)
            data[r['LogicalResourceId']]['result']['y'][0].append(
                r['LogicalResourceId'])
            data[r['LogicalResourceId']]['result']['y'][1].append(
                r['ResourceStatus'])

    fig = go.Figure()
    for k, v in data.items():
        fig.add_trace(go.Waterfall(orientation='h', **v['result']))
    fig.update_layout(title={'text': f'<span style="color:#000000">CloudFormation Waterfall - {stackname}</span>'},
                      showlegend=False,
                      height=(len(data)*30),
                      font={
                          'family': 'Open Sans, light',
                          'color': 'black',
                          'size': 14
                      },
                      plot_bgcolor='rgba(0,0,0,0)')
    fig.update_xaxes(tickangle=-45, tickfont=dict(family='Open Sans, light', color='black', size=14))
    fig.update_yaxes(tickangle=0, tickfont=dict(family='Open Sans, light', color='black', size=14))
    fig.show()


if __name__ == '__main__':
    fire.Fire(main)
