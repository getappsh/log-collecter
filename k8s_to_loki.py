import os
import time
import json
import requests
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

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
        print("Loaded in-cluster Kubernetes configuration.")
    except config.ConfigException:
        config.load_kube_config()
        print("Loaded local Kubernetes configuration.")
    return client.CoreV1Api()


def get_pod_logs(v1_api, pod_name, container_name, namespace, since_time=None):
    """Fetch logs for a specific container in a pod."""
    try:
        print(f"Fetching logs for pod: {pod_name}, container: {container_name}, since: {since_time}")
        logs = v1_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container_name,
            since_time=since_time,
            timestamps=True,
        )
        print(f"Fetched logs for {pod_name}/{container_name}: {len(logs.splitlines())} lines.")
        return logs
    except ApiException as e:
        print(f"Error fetching logs for {pod_name}/{container_name}: {e}")
        return ""


def send_logs_to_loki(log_lines, pod_name, container_name):
    """Send logs to Loki."""
    streams = []
    for line in log_lines:
        try:
            timestamp, message = line.split(" ", 1)
            # Loki requires nanosecond precision for timestamps
            nanosecond_timestamp = str(
                int(time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"))) * 1_000_000_000
            )
            streams.append({
                "stream": {
                    "namespace": NAMESPACE,
                    "pod": pod_name,
                    "container": container_name,
                },
                "values": [[nanosecond_timestamp, message]],
            })
        except ValueError as e:
            print(f"Error parsing log line: {line}, error: {e}")

    if streams:
        payload = {"streams": streams}
        try:
            print(f"Sending {len(streams)} log entries to Loki.")
            response = requests.post(
                LOKI_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
            )
            print(f"Loki response: {response.status_code}, {response.text}")
        except requests.RequestException as e:
            print(f"Error sending logs to Loki: {e}")


def main():
    """Main function."""
    v1_api = get_k8s_client()
    while True:
        print("Fetching logs...")
        try:
            pods = v1_api.list_namespaced_pod(namespace=NAMESPACE)
            print(f"Found {len(pods.items)} pods in namespace '{NAMESPACE}'.")
            for pod in pods.items:
                pod_name = pod.metadata.name
                for container in pod.spec.containers:
                    container_name = container.name
                    since_time = log_positions.get(f"{pod_name}/{container_name}")
                    print(f"Processing pod: {pod_name}, container: {container_name}")
                    logs = get_pod_logs(v1_api, pod_name, container_name, NAMESPACE, since_time)
                    if logs:
                        log_lines = logs.strip().split("\n")
                        send_logs_to_loki(log_lines, pod_name, container_name)
                        # Update the last timestamp
                        if log_lines:
                            last_line = log_lines[-1]
                            try:
                                log_positions[f"{pod_name}/{container_name}"] = last_line.split(" ", 1)[0]
                                print(f"Updated last timestamp for {pod_name}/{container_name}: {log_positions[f'{pod_name}/{container_name}']}")
                            except ValueError as e:
                                print(f"Error updating last timestamp for {pod_name}/{container_name}: {e}")
                    else:
                        print(f"No logs found for {pod_name}/{container_name}.")
        except Exception as e:
            print(f"Error fetching or processing logs: {e}")
        print("Sleeping for the next iteration...")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
