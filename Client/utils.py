
import socket
def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)  # Set a short timeout for the check
        return s.connect_ex(('localhost', port)) != 0  # Returns True if port is available
