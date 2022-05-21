#!/usr/bin/env python
import argparse
import requests
import os
import socket
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark consul")
    parser.add_argument("-t","--type", required=True, help="up/down")
    parser.add_argument("-n","--number", required=True, help="number of ports")
    parser.add_argument("-s","--start", required=True, help="starting port number")

    args = parser.parse_args()

    register_url = "http://127.0.0.1:2280/v1/agent/service/register"
    deregister_url = "http://127.0.0.1:2280/v1/agent/service/deregister"
    
    start = int(args.start)
    number = int(args.number)
    psm = "data.cnch.benchmark_vw"
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)

    if args.type == "up":
        for i in range(start,start+number):
            payload = {
                "name": psm,
                "id": "{}-{}".format(psm, i),
                "address": ip,
                "port": i,
                "tags": ["hostname:cnch-default-workers-default-0.cnch-workers-default-headless.cnch.svc.cluster.local.","cluster:default","vw_name:vw_default","env:prod","PORT1:6007","PORT2:6008","PORT5:6009","PORT6:6010"],
            }
            response = requests.put(
                register_url, json=payload, headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
    else:
        for i in range(start,start+number):
            response = requests.put('{}/{}-{}'.format(deregister_url,psm,i))