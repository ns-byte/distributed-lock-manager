
import os
import json
import time
import socket


# Define the log file path

def log_event(event,server_id):
    """Append an event to the log file."""
    LOG_FILE = f"Server/server_{server_id}_log.json"
    with open(LOG_FILE, "a") as f:
        timestamped_event = {"timestamp": time.time(), "event": event}
        f.write(json.dumps(timestamped_event) + "\n")

def load_server_state(server,server_id):
    """Load the server's state from the log file on restart."""
    LOG_FILE = f"Server/server_{server_id}_log.json"
    if not os.path.exists(LOG_FILE):
        return  # No log file means no state to restore

    with open(LOG_FILE, "r") as f:
        for line in f:
            event_data = json.loads(line.strip())
            event = event_data["event"]

            # Restore server state based on each logged event
            if event.startswith("Client initialized"):
                client_id = int(event.split(": ")[1])
                server.next_client_id = max(server.next_client_id, client_id + 1)
            elif event.startswith("Lock acquired"):
                client_id_part = event.split(": ")[1]         
                client_id = int(client_id_part)
                server.current_lock_holder = client_id
            elif event.startswith("Lock released") or event.startswith("Lock automatically released"):
                server.current_lock_holder = None
            elif event.startswith("Client added to waiting queue"):
                client_id = int(event.split(": ")[1])
                if client_id not in [c[0] for c in server.waiting_queue]:
                    server.waiting_queue.append((client_id, None))  # 'None' for peer
            elif event.startswith("Lock granted to next client in queue"):
                parts = event.split(": ")
                client_id_part = parts[1]
                client_id = int(client_id_part)
                server.current_lock_holder = client_id
            # elif event.startswith("logged heartbeat"):
            #     parts=event.split(", ")
            #     client_id = int(parts[0].split(" : ")[1])
            #     heartbeat_interval = float(parts[1])
            #     server.heartbeat_intervals[client_id] = heartbeat_interval

def is_duplicate_request(request_id):
    """Check if the request ID has been processed already."""
    if not hasattr(is_duplicate_request, "processed_requests"):
        is_duplicate_request.processed_requests = set()

    return request_id in is_duplicate_request.processed_requests

def mark_request_processed(request_id):
    """Mark the request ID as processed."""
    if not hasattr(is_duplicate_request, "processed_requests"):
        is_duplicate_request.processed_requests = set()
    is_duplicate_request.processed_requests.add(request_id)

def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)  # Set a short timeout for the check
        return s.connect_ex(('localhost', port)) != 0  # Returns True if port is available


def write_logs(content,server_id):
    LOG_FILE = f"Server/server_{server_id}_log.json"
    with open(LOG_FILE, "w") as f:
        f.write(content)
 
def get_log_content(server_id):
    """Read all log entries for a specific server and return them as a single formatted string."""
    LOG_FILE = f"Server/server_{server_id}_log.json"
    log_entries = []

    try:
        with open(LOG_FILE, "r") as f:
            return f.read()
    except FileNotFoundError:
        print(f"Log file for server {server_id} not found.")
    
