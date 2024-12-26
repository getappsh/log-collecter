import os
import time
import json
import requests
import re
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from datetime import datetime, timezone

# Configuration
LOKI_URL = os.getenv("LOKI_URL", "http://loki.chart-test.svc.cluster.local:3100/loki/api/v1/push")
NAMESPACE = os.getenv("NAMESPACE", "default")
INTERVAL = int(os.getenv("INTERVAL", 60))  # Interval to fetch logs in seconds
CONTAINER_NAME = "api"  # Hardcoded to only process 'api' container

# Track the last log timestamp for the api container
last_timestamps = {}

def get_k8s_client():
    """Initialize Kubernetes API client."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client.CoreV1Api()

def calculate_seconds_since(timestamp_str):
    """Calculate seconds elapsed since the given timestamp."""
    if not timestamp_str:
        return None
    try:
        last_time = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        seconds = int((now - last_time).total_seconds())
        return max(1, seconds)  # Ensure we don't return 0 or negative values
    except Exception as e:
        print(f"Error calculating time difference: {e}")
        return None

def get_pod_logs(v1_api, pod_name, namespace, since_seconds=None):
    """Fetch logs for the api container in a pod."""
    try:
        return v1_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=CONTAINER_NAME,
            timestamps=True,
            since_seconds=since_seconds,
            tail_lines=None  # Get all available logs since last check
        )
    except ApiException as e:
        print(f"Error fetching logs for {pod_name}/{CONTAINER_NAME}: {e}")
        return ""

def send_logs_to_loki(log_lines, pod_name):
    for line in log_lines:
        try:
            # Split timestamp from the log line
            parts = line.split(' ', 1)
            if len(parts) != 2:
                continue
                
            timestamp, message = parts
            
            if not message.strip():
                continue
                
            clean_line = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', message)
            clean_line = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', clean_line)
            clean_line = clean_line.replace('"', '\\"').replace("'", "\\'")

            # Convert timestamp to nanoseconds for Loki
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            nano_timestamp = str(int(dt.replace(tzinfo=timezone.utc).timestamp() * 1e9))

            payload = {
                "streams": [{
                    "stream": {
                        "namespace": "chart-test",
                        "pod": pod_name,
                        "container": CONTAINER_NAME
                    },
                    "values": [
                        [nano_timestamp, clean_line]
                    ]
                }]
            }

            response = requests.post(
                LOKI_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=5
            )

            if response.status_code not in [200, 204]:
                print(f"Error {response.status_code}: {response.text}")
            elif response.status_code == 204:
                print(f"API log sent successfully: {clean_line[:50]}...")

        except Exception as e:
            print(f"Error processing API log: {str(e)}")
            continue                        

def main():
    """Main function."""
    v1_api = get_k8s_client()
    
    while True:
        print("Fetching API container logs...")
        try:
            pods = v1_api.list_namespaced_pod(namespace=NAMESPACE)
            
            for pod in pods.items:
                # Check if pod has api container
                if CONTAINER_NAME in [container.name for container in pod.spec.containers]:
                    pod_name = pod.metadata.name
                    pod_key = f"{pod_name}/{CONTAINER_NAME}"
                    
                    # Calculate seconds since last timestamp
                    since_seconds = calculate_seconds_since(last_timestamps.get(pod_key))
                    logs = get_pod_logs(v1_api, pod_name, NAMESPACE, since_seconds)
                    
                    if logs:
                        log_lines = logs.strip().split("\n")
                        if log_lines and log_lines[0]:  # Check if we have valid log lines
                            send_logs_to_loki(log_lines, pod_name)
                            
                            # Update the last timestamp based on the last log line
                            last_line = log_lines[-1]
                            timestamp = last_line.split(' ', 1)[0]
                            last_timestamps[pod_key] = timestamp

        except Exception as e:
            print(f"Error fetching or processing API logs: {e}")

        print(f"Sleeping for {INTERVAL} seconds...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()