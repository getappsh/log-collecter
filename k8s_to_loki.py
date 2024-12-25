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
    except config.ConfigException:
        config.load_kube_config()
    return client.CoreV1Api()


def get_pod_logs(v1_api, pod_name, container_name, namespace, since_time=None):
    """Fetch logs for a specific container in a pod."""
    try:
        return v1_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            container=container_name,
            since_time=since_time,
            timestamps=True,
        )
    except ApiException as e:
        print(f"Error fetching logs for {pod_name}/{container_name}: {e}")
        return ""


def send_logs_to_loki(log_lines, pod_name, container_name):
    """Send logs to Loki."""
    streams = []
    for line in log_lines:
        timestamp, message = line.split(" ", 1)
        # Loki requires nanosecond precision for timestamps
        nanosecond_timestamp = str(int(time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"))) * 1_000_000_000)
        streams.append({
            "stream": {
                "namespace": NAMESPACE,
                "pod": pod_name,
                "container": container_name,
            },
            "values": [[nanosecond_timestamp, message]],
        })

    if streams:
        payload = {"streams": streams}
        try:
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
            for pod in pods.items:
                pod_name = pod.metadata.name
                for container in pod.spec.containers:
                    container_name = container.name
                    since_time = log_positions.get(f"{pod_name}/{container_name}")
                    logs = get_pod_logs(v1_api, pod_name, container_name, NAMESPACE, since_time)
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
