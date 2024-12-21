from Proto import lock_pb2
from Proto import lock_pb2_grpc
import grpc
import time
import asyncio
import threading
from .utils import is_port_available
import sys

SWITCH_SERVER_TIMEOUT = 25
MAXIMUM_RETRIES = 60
class Client:
    def __init__(self):
        self.client_id = None # Client ID assigned by the server
        self.request_counter = 0 # Counter for generating request IDs
        self.stop_heartbeat = False # Flag to stop the heartbeat thread
        self.ports_to_try = ["50051", "50052", "50053","50054"]  # Define additional ports to try
        self.lock_held = False # Flag to indicate if the client holds the lock
        self.connection_closed = False # Flag to indicate if the connection is closed
 

    def generate_request_id(self):
        # Generate a unique request ID for RPC calls
        self.request_counter += 1
        return f"{self.client_id}-{self.request_counter}"

    def RPC_init(self):
        # Initialize connection to the primary server
        flag = True
        for port in self.ports_to_try:
            if is_port_available(int(port)):
                continue
            try:
                print(f"Attempting to connect to localhost:{port}...")
                self.channel = grpc.insecure_channel(f'localhost:{port}')
                self.stub = lock_pb2_grpc.LockServiceStub(self.channel)
                print(f"Connected to server on port {port}")
                request = lock_pb2.Int()
                response = self.stub.client_init(request)

                # Check if connected to the primary server
                if(response.rc != -1):
                    flag = False
                break

            except Exception as e:
                flag = True
                print(f"Connection to localhost:{port} failed. Trying the next port.")
        
        if flag:
            raise ConnectionError("Could not connect to any of the specified ports.")

        print("Initialized connection with server:", response)
        self.client_id = response.rc # Assign client ID from the server
        print(f"Assigned client with id: {self.client_id}")

    def RPC_lock_acquire(self):
        # Attempt to acquire the lock from the server
        retry_interval = 5
        timeout=5
        request_id = self.generate_request_id()
        
        # Start the heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
        
        while True:
            try:
                response = self.stub.lock_acquire(lock_pb2.lock_args(
                    client_id=self.client_id,
                    request_id=request_id
                ))
                timeout = 5
                retry_interval = 5
                if response.status == lock_pb2.Status.SUCCESS:
                    print(f"Lock has been acquired by client: {self.client_id}")
                    self.lock_held = True
                    break
                elif response.status == lock_pb2.Status.DUPLICATE_ERROR:
                    print("Your request is being processed.")
                else:
                    print(f"Client {self.client_id} is waiting in queue.")
                    time.sleep(retry_interval)
                
            except grpc.RpcError:
                print(f"Server is unavailable. Retrying in {retry_interval} seconds...")
                if timeout >= SWITCH_SERVER_TIMEOUT:
                    print(f"Maximum retries reached, switching to different server")
                    self.switch_server()
                    continue
                time.sleep(retry_interval)
                timeout +=5
                retry_interval = min(retry_interval + 2, 20)

    def RPC_lock_release(self):
        # Release the lock held by the client
        retry_interval = 5
        request_id = self.generate_request_id()
        timeout=5
        while True:
            try:
                response = self.stub.lock_release(lock_pb2.lock_args(
                    client_id=self.client_id,
                    request_id=request_id
                ))
                if response.status == lock_pb2.Status.SUCCESS:
                    print(f"Lock has been released by client: {self.client_id}")
                    # Stop the heartbeat thread
                    self.lock_held = False
                    self.stop_heartbeat = True
                    if hasattr(self, 'heartbeat_thread'):
                        self.heartbeat_thread.join()
                    break
                else:
                    print(f"Lock cannot be released by client: {self.client_id} as it doesn't hold the lock")
                    break

            except grpc.RpcError:
                print(f"Server is unavailable. Retrying in {retry_interval} seconds...")
                if timeout >= SWITCH_SERVER_TIMEOUT:
                    print(f"Maximum retries reached, switching to different server")
                    self.switch_server()
                    continue
                timeout +=5
                time.sleep(retry_interval)
                retry_interval = min(retry_interval + 2, 20)  # Exponential backoff with a max wait time

    def send_heartbeats(self):
        # Periodically send heartbeats to the server
        while not self.stop_heartbeat:
            try:
                time.sleep(5)  # Send heartbeat every 5 seconds
                response = self.stub.heartbeat(lock_pb2.Heartbeat(client_id=self.client_id))
                if response.status == lock_pb2.Status.TIMEOUT_ERROR:
                    print("Received TIMEOUT_ERROR: Lock released by server due to timeout.")
                    self.stop_heartbeat = True
                    self.lock_held = False
                    self.RPC_close()
                    self.connection_closed = True
                    break
            except grpc.RpcError as e:
                print("Failed to send heartbeat: Server may be unavailable.")
                break
    def is_server_alive(self,server):
        try:
           channel = grpc.insecure_channel(server)
           stub = lock_pb2_grpc.LockServiceStub(channel)
           response = stub.Ping(lock_pb2.Empty())
           print(f"Ping response from {server}: {response.status}")
           return response.status == "OK"
        except grpc.RpcError as e:
           print(f"Failed to connect to {server}: {e}")
           return False

    def switch_server(self):
        # Switch to a backup server if the current server is unavailable
        servers = ['localhost: 50052','localhost: 50053','localhost: 50054','localhost: 50051']
        retries = 0
        flag = True
        while True:
            if retries >= MAXIMUM_RETRIES:
                print(f"No server is available")
            for server in servers:
                try:
                    if self.is_server_alive(server):
                      self.channel = grpc.insecure_channel(server)
                      self.stub = lock_pb2_grpc.LockServiceStub(self.channel)
                      flag = False
                      self.stub = lock_pb2_grpc.LockServiceStub(self.channel)
                      break
                except grpc.RpcError:
                    print(f"Unable to connect to server at {server}, trying next server")
            if flag:
                retries=retries+4
                print("No server available, trying again")
                continue
            print(f"Connection successful with new server")
            break
        if flag :
            self.RPC_close()  # Cleanup before exiting
            sys.exit("Exiting: No available servers.")


    def append_file(self, filename, content):
        # Append content to a file on the server
        retry_interval = 5
        request_id = self.generate_request_id()
        timeout = 5
        while True:
                try:
                    if not client.connection_closed:
                        lock_holder_response = self.stub.getCurrent_lock_holder(lock_pb2.current_lock_holder(client_id=self.client_id))
                        if self.client_id==lock_holder_response.current_lock_holder.client_id:
                            response=self.stub.file_append(lock_pb2.file_args(filename=filename,content=content.encode(),client_id=self.client_id,request_id=request_id))
                            timeout = 5
                            retry_interval = 5
                            if response.status== lock_pb2.Status.SUCCESS:
                                print(f"File has been appended by client {self.client_id} ")
                                break
                            elif response.status== lock_pb2.Status.DUPLICATE_ERROR:
                                print("Your query is being processed.")
                                time.sleep(retry_interval)
                                retry_interval = min(retry_interval + 2, 30)
                            else:
                                print("Failed to append file.")
                                break
                        else:
                            client.RPC_lock_acquire()
                    else:
                        break
                except grpc.RpcError:
                    if timeout >= SWITCH_SERVER_TIMEOUT:
                      print(f"Maximum retries reached, switching to different server")
                      self.switch_server()
                      continue
                    time.sleep(retry_interval)
                    timeout +=5
                    retry_interval = min(retry_interval + 2, 20)                     

    def RPC_close(self):
        # Close the RPC connection and clean up resources
        if self.connection_closed:
            print("RPC connection is already closed. Skipping...")
            return
        
        self.connection_closed = True
        print(f"Closing RPC connection for client {self.client_id}...")
        if self.lock_held:
            try:
                if hasattr(self, 'channel') and self.channel._channel.check_connectivity_state(True) != grpc.ChannelConnectivity.SHUTDOWN:
                    print("Releasing lock before closing...")
                    self.RPC_lock_release()
                else:
                    print("Cannot release lock: Channel is inactive or closed.")
            except grpc.RpcError as e:
                print(f"Failed to release lock due to: {e.details()}")
            except ValueError as e:
                print(f"Error during lock release: {e}")

        if hasattr(self, 'channel') and self.channel:
            try:
                self.channel.close()
                print("gRPC channel closed.")
            except Exception as e:
                print(f"Failed to close gRPC channel: {e}")

        print("Client shutdown complete.")
        return
    
    
if __name__ == '__main__':
    base_directory = "Server/Files/"
    client = Client()
    try:
        client.RPC_init()
        client.RPC_lock_acquire()
        time.sleep(15)
        file_path = "./Server/Files/file_0"
        client.append_file(filename=file_path, content='A')
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Graceful shutdown
        if not client.connection_closed:
            client.RPC_close()