import os
import time
import json
import requests
import re
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from datetime import datetime

# Configuration
LOKI_URL = os.getenv("LOKI_URL", "http://loki.chart-test.svc.cluster.local:3100/loki/api/v1/push")
NAMESPACE = os.getenv("NAMESPACE", "default")
INTERVAL = int(os.getenv("INTERVAL", 60))  # Interval to fetch logs in seconds

# Track the last log timestamps for each container
log_positions = {}

def get_k8s_client():
    """Initialize Kubernetes API client."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client.CoreV1Api()

def get_pod_logs(v1_api, pod_name, container_name, namespace, since_seconds=None):
    """Fetch logs for a specific container in a pod."""
    try:
        return v1_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container_name,
            timestamps=True,
            since_seconds=since_seconds
        )
    except ApiException as e:
        print(f"Error fetching logs for {pod_name}/{container_name}: {e}")
        return ""

def clean_log_line(line):
    """Clean the log line using regex to remove color codes and control characters."""
    # Remove color codes (e.g., \x1b[39m) and control characters
    clean_line = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', line)
    clean_line = re.sub(r'[[:cntrl:]]', '', clean_line)
    clean_line = re.sub(r'"', r'\"', clean_line)
    clean_line = re.sub(r"'", r"\\'", clean_line)
    return clean_line

def send_logs_to_loki(log_lines, pod_name, container_name):
    """Send logs to Loki."""
    streams = []
    for line in log_lines:
        timestamp, message = line.split(" ", 1)
        try:
            # Clean the log line
            clean_message = clean_log_line(message)

            # Convert the timestamp to nanoseconds (string format)
            timestamp = timestamp.rstrip('Z')
            timestamp_parts = timestamp.split('.')
            seconds = timestamp_parts[0]
            nanoseconds = timestamp_parts[1].ljust(9, '0')  # Ensure 9 digits for nanoseconds
            nanosecond_timestamp = f"{seconds}{nanoseconds}"

            # Create the payload
            payload = {
                "streams": [
                    {
                        "stream": {
                            "namespace": NAMESPACE,
                            "pod": pod_name,
                            "container": container_name,
                        },
                        "values": [
                            [nanosecond_timestamp, clean_message]
                        ]
                    }
                ]
            }

            # Send the log to Loki
            response = requests.post(
                LOKI_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
            )
            print(f"Loki response: {response.status_code}, {response.text}")

        except Exception as e:
            print(f"Error processing log line: {e}")
            continue

def main():
    """Main function."""
    v1_api = get_k8s_client()
    while True:
        print("Fetching logs...")
        try:
            pods = v1_api.list_namespaced_pod(namespace=NAMESPACE)
            for pod in pods.items:
                pod_name = pod.metadata.name
                for container in pod.spec.containers:
                    container_name = container.name

                    # Determine the last known timestamp for this pod/container
                    last_timestamp = log_positions.get(f"{pod_name}/{container_name}")
                    since_seconds = None
                    if last_timestamp:
                        last_timestamp_dt = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                        now_dt = datetime.utcnow()
                        delta = now_dt - last_timestamp_dt
                        since_seconds = int(delta.total_seconds())

                    logs = get_pod_logs(v1_api, pod_name, container_name, NAMESPACE, since_seconds)
                    if logs:
                        log_lines = logs.strip().split("\n")
                        send_logs_to_loki(log_lines, pod_name, container_name)

                        # Update the last timestamp
                        if log_lines:
                            last_line = log_lines[-1]
                            log_positions[f"{pod_name}/{container_name}"] = last_line.split(" ", 1)[0]

        except Exception as e:
            print(f"Error fetching or processing logs: {e}")

        print("Sleeping for the next iteration...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
