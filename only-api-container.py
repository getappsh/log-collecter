def send_api_logs_to_loki(log_lines, pod_name):
    # Only process logs for the "api" container
    container_name = "api"
    
    for line in log_lines:
        try:
            if not line.strip():
                continue
            
            clean_line = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', line)
            clean_line = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', clean_line)
            clean_line = clean_line.replace('"', '\\"').replace("'", "\\'")
            
            payload = {
                "streams": [{
                    "stream": {
                        "namespace": "chart-test",
                        "pod": pod_name,
                        "container": container_name
                    },
                    "values": [
                        [str(int(time.time() * 1e9)), clean_line]
                    ]
                }]
            }
            
            print(f"Sending API container payload: {json.dumps(payload)[:200]}")  # Debug log
            
            response = requests.post(
                LOKI_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=5
            )
            
            if response.status_code not in [200, 204]:
                print(f"Error sending API log {response.status_code}: {response.text}")
            elif response.status_code == 204:
                print(f"API log sent successfully: {clean_line[:50]}...")
                
        except Exception as e:
            print(f"Error processing API log: {str(e)}")
            continue