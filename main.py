import datetime
import json
from typing import Optional, Union
from urllib.parse import urljoin

import numpy as np
import pandas as pd

Timestamp = Union[str, float, datetime.datetime]  # RFC-3339 string or as a Unix timestamp in seconds
Duration = Union[str, datetime.timedelta]  # Prometheus duration string
Matrix = pd.DataFrame
Vector = pd.Series
Scalar = np.float64
String = str

cpu = ["calico_node", "docker", "kernel",
       "kube_proxy", "kubelet", "loki",
       "node_exporter", "node_problem_detector"]

memory = ["calico_node", "docker",
          "kube_proxy", "kubelet", "loki",
       "node_exporter", "node_problem_detector"]

metrics_path = "metrics"


def metric_name(metric: dict) -> str:
    """Convert metric labels to standard form."""
    name = metric.get('__name__', '')
    labels = ','.join(('{}={}'.format(k, json.dumps(v)) for k, v in metric.items() if k != '__name__'))
    return '{0}{{{1}}}'.format(name, labels)

def to_pandas(data: dict) -> Union[Matrix, Vector, Scalar, String]:
    """Convert Prometheus data object to Pandas object."""
    result_type = data['resultType']
    if result_type == 'vector':
        return pd.Series((np.float64(r['value'][1]) for r in data['result']),
                         index=(metric_name(r['metric']) for r in data['result']))
    elif result_type == 'matrix':
        return pd.DataFrame({
            metric_name(r['metric']):
                pd.Series((np.float64(v[1]) for v in r['values']),
                          index=(pd.Timestamp(v[0], unit='s') for v in r['values']))
            for r in data['result']})
    elif result_type == 'scalar':
        return np.float64(data['result'])
    elif result_type == 'string':
        return data['result']
    else:
        raise ValueError('Unknown type: {}'.format(result_type))

def process_cpu():
    print("------------------process cpu-----------------------")
    print("metrics", "0.5", "0.9", "0.99", "Max")
    for item in cpu:
        path = "metrics/cpu/" + item
        with open(path, 'r') as f:
            temp = json.loads(f.read())
            pd = to_pandas(temp['data'])
            print(item, pd.quantile(.5)[1], pd.quantile(.9)[1], pd.quantile(.99)[1], pd.max()[1].max())
    print("------------------Done-----------------------")

def process_mem():
    print("------------------process memory-----------------------")
    print("metrics", "0.5", "0.9", "0.99", "Max")
    for item in memory:
        path = "metrics/memory/" + item
        with open(path, 'r') as f:
            temp = json.loads(f.read())
            pd = to_pandas(temp['data'])
            print(item, pd.quantile(.5)[1]/(1024*1024), pd.quantile(.9)[1]/(1024*1024), 
            pd.quantile(.99)[1]/(1024*1024), pd.max()[1].max()/(1024*1024))
    print("------------------Done-----------------------")


process_cpu()
process_mem()
