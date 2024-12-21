
from concurrent import futures
import os
import grpc
import threading
from collections import deque
import asyncio
import time
from Proto import lock_pb2
from Proto import lock_pb2_grpc
import sys
import signal
from .utils import load_server_state, log_event, is_duplicate_request, mark_request_processed, is_port_available,get_log_content,write_logs

HEARTBEAT_INTERVAL = 5  # Heartbeat interval in seconds
HEARTBEAT_TIMEOUT = 20  # Timeout threshold for failover
PRIMARY_SERVER = "primary"
BACKUP_SERVER = "backup"

class LockServiceServicer(lock_pb2_grpc.LockServiceServicer):
    def __init__(self, server_id, peers, role=BACKUP_SERVER):
        self.server_id = server_id
        self.peers = sorted(peers)  # Ensure peers are sorted to establish FIFO order
        self.role = role  # Role of the server (primary/backup)
        self.current_lock_holder = None  # Current lock holder's client ID
        self.waiting_queue = deque()  # Queue for clients waiting for the lock
        self.next_client_id = 1  # ID to assign to the next client
        self.lock = threading.Lock()  # Synchronization lock
        self.heartbeat_intervals = {}  # Last heartbeat time for each client
        self.backup_servers = ["localhost:50051","localhost:50052", "localhost:50053", "localhost:50054"]  # Backup server addresses
        self.last_primary_heartbeat = time.time()  # Timestamp of the last heartbeat from primary
        self.current_term = 0 # Raft-style election term
        self.voted_for = None  # ID of the candidate this server voted for
        self.votes_received = 0  # Count of votes received during an election
        self.current_address = 'localhost:50051' # Current server address
        self.append_request_cache = {}  # Cache for append requests
        self.active_backups_server = {} # Active backup server stubs
        self.completed_operations_cache = {} # Cache for completed operations
        self.lock_acquire_time = {} # Record lock acquisition time

        # Load state on startup
        load_server_state(self,self.current_address[-1])
        
        # Start leader election or heartbeat checking based on role
        if self.role == PRIMARY_SERVER:
            threading.Thread(target=self.send_heartbeats_to_backups, daemon=True).start()
        else:
            threading.Thread(target=self.check_primary_heartbeat, daemon=True).start()

    def start_election(self):
        # Initiate an election process in a FIFO order to become the primary server
        position = self.peers.index(self.server_id)
        delay = position
        
        #Ensuring if the previous server ports are down, take the current server port has highest priority that is without any delay
        for i in range(0,position):
            if is_port_available(int(peers[i])):
                delay = delay-1
                print(delay)
        election_delay = delay * delay * 10  # Delay election attempt based on position in FIFO order
        
        print(f"Server {self.server_id} waiting {election_delay} seconds before election attempt.")
        time.sleep(election_delay)

        # Recheck primary status before starting the election
        if time.time() - self.last_primary_heartbeat <= HEARTBEAT_TIMEOUT:
            print(f"Server {self.server_id} detected heartbeat from primary; aborting election.")
            return

        # Start election
        self.current_term += 1
        self.voted_for = self.server_id
        self.votes_received = 1  # Vote for self
        print(f"Server {self.server_id} started election for term {self.current_term}")

        # Request votes from peers
        for backup, address in self.active_backups_server.items():
            if backup != self.server_id and not is_port_available(int(backup)):
                threading.Thread(target=self.request_vote, args=(address,), daemon=True).start()
            if is_port_available(int(backup)):
                self.votes_received+=1

        # Election timeout to check if majority vote is achieved
        time.sleep(0.1)
        if self.votes_received >= len(self.active_backups_server):
            self.become_primary()
            return

        # If not elected, revert to follower
        self.role = BACKUP_SERVER
        print(f"Server {self.server_id} reverting to follower after unsuccessful election.")

    def request_vote(self, peer):
        # Request a vote from a peer during an election
        try:
            vote_request = lock_pb2.VoteRequest(candidate_id=self.server_id, term=self.current_term)
            response = peer.vote(vote_request)
            
            if response.vote_granted:
                self.votes_received += 1
                print(f"Server {self.server_id} received a vote from {peer}. Total votes: {self.votes_received}")
        except grpc.RpcError:
            print(f"Failed to request vote from {peer}")

    def vote(self, request, context):
        # Respond to a vote request from a candidate server
        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = request.candidate_id
            print(f"Server {self.server_id} voted for {request.candidate_id} in term {self.current_term}")
            return lock_pb2.VoteResponse(vote_granted=True)
        return lock_pb2.VoteResponse(vote_granted=False)

    def become_primary(self):
        # Promote this backup server to primary
        self.role = PRIMARY_SERVER
        print(f"Server {self.server_id} is now the primary.")
        load_server_state(self,self.current_address[-1])  
        threading.Thread(target=self.send_heartbeats_to_backups, daemon=True).start()
        threading.Thread(target=self.check_heartbeats, daemon=True).start()


    def check_primary_heartbeat(self):
        # Backup server checks for primary's heartbeat
        while self.role == BACKUP_SERVER:
            time.sleep(HEARTBEAT_INTERVAL)
            print("primary is active")
            if time.time() - self.last_primary_heartbeat > HEARTBEAT_TIMEOUT:
                print("Primary server heartbeat missed. Initiating election.")
                self.start_election()
                continue

    def send_heartbeats_to_backups(self):
        # Primary server sends heartbeats to backups
        backup_servers = ["localhost:50051","localhost:50052", "localhost:50053", "localhost:50054"]
        while self.role == PRIMARY_SERVER:
            time.sleep(HEARTBEAT_INTERVAL)
            for backup_address in backup_servers:
                if backup_address == self.current_address:
                    continue
                try:
                    channel = grpc.insecure_channel(backup_address)
                    stub = lock_pb2_grpc.LockServiceStub(channel)
                    stub.heartbeat(lock_pb2.Heartbeat(client_id=0))
                    print(f"Heartbeat sent to backup at {backup_address}")
                    if self.active_backups_server.get(backup_address) == None:
                        self.active_backups_server[backup_address] = stub
                        time.sleep(5)
                        threading.Thread(target=self.sync_logs_with_backup, args=(backup_address, stub), daemon=True).start()
                        threading.Thread(target=self.sync_backup_files, args=(backup_address, stub), daemon=True).start()
                except grpc.RpcError:
                    if self.active_backups_server.get(backup_address) != None:
                        self.active_backups_server.pop(backup_address)
                    print(f"Failed to send heartbeat to backup at {backup_address}")


    def heartbeat(self, request, context):
        # Handle heartbeat requests from primary or clients
        if self.role == BACKUP_SERVER:
            self.last_primary_heartbeat = time.time()
        else:
            client_id = request.client_id
            with self.lock:
                self.heartbeat_intervals[client_id] = time.time()
                if self.current_lock_holder == client_id:
                    lock_acquire_time = self.lock_acquire_time.get(client_id)
                    if (time.time() - lock_acquire_time) > 120:
                        event = f"Lock hold timeout for client: {self.current_lock_holder}"
                        log_event(event,self.current_address[-1])
                        self.current_lock_holder = None
                        
                        if self.waiting_queue:
                            next_client_id, _ = self.waiting_queue.popleft()
                            self.current_lock_holder = next_client_id
                            self.heartbeat_intervals[next_client_id] = time.time()
                            event = f"Lock granted to next client in queue: {next_client_id}"
                            log_event(event,self.current_address[-1])
                        del self.heartbeat_intervals[client_id]
                        return lock_pb2.Response(status=lock_pb2.Status.TIMEOUT_ERROR)
        return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)

    def client_init(self, request, context):
        # Initialize a client connection and assign a client ID
        with self.lock:
            if self.role != PRIMARY_SERVER:
                print("Got client request, trying to become primary")
                time.sleep(30)
            if self.role != PRIMARY_SERVER:
                return lock_pb2.Int(rc = -1)
            client_id = self.next_client_id
            self.next_client_id += 1
            event = f"Client initialized with client_id: {client_id}"
            log_event(event,self.current_address[-1])
            self.log_event_to_backup(event)
        return lock_pb2.Int(rc=client_id)
    
    def lock_acquire(self, request, context):
        # Handle lock acquisition requests from clients
        client_id = request.client_id
        request_id = request.request_id
        peer = context.peer()
        self.lock_acquire_time[client_id] = time.time()

        with self.lock:
            if is_duplicate_request(request_id):
                if self.current_lock_holder and self.current_lock_holder == client_id:
                    return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
                else:
                    return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)


            # Record the request_id as processed
            mark_request_processed(request_id)

            if self.current_lock_holder and self.current_lock_holder == client_id:
                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)

            if self.current_lock_holder is not None:
                if (client_id, peer) not in self.waiting_queue:
                    self.waiting_queue.append((client_id, peer))
                event = f"Client added to waiting queue : {client_id}"
                log_event(event,self.current_address[-1])
                print("Sending logs to backup")
                self.log_event_to_backup(event)
 
                return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)

            self.current_lock_holder = client_id
            self.heartbeat_intervals[client_id] = time.time()  # Track heartbeat time
            event = f"Lock acquired by client: {client_id}"
            log_event(event,self.current_address[-1])
            self.log_event_to_backup(event)
 
            return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
        
    def lock_release(self, request, context):
         # Handle lock release requests from clients
        client_id = request.client_id
        request_id = request.request_id

        with self.lock:
            if is_duplicate_request(request_id) and self.current_lock_holder != client_id and self.heartbeat_intervals.get(client_id) is None:
                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
            

            mark_request_processed(request_id)

            if self.current_lock_holder and self.current_lock_holder == client_id:
                self.current_lock_holder = None
                event = f"Lock released by client: {client_id}"
                log_event(event,self.current_address[-1])
                self.log_event_to_backup(event)

                if self.waiting_queue:
                    next_client_id, next_peer = self.waiting_queue.popleft()
                    self.current_lock_holder = next_client_id
                    self.heartbeat_intervals[next_client_id] = time.time()
                    event = f"Lock granted to next client in queue: {next_client_id}"
                    log_event(event,self.current_address[-1])
                    self.log_event_to_backup(event)


                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
            else:
                return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
     
    def check_heartbeats(self):
        # Periodically check heartbeats to release lock if the client is inactive
        while True:
            time.sleep(1)
            current_time = time.time()
            
            with self.lock:
                # General timeout check       
                if self.current_lock_holder != None and self.heartbeat_intervals.get(self.current_lock_holder) is None:
                    #Inital hearbeat after recovery
                    self.heartbeat_intervals[self.current_lock_holder] = time.time()           
                for client_id, last_heartbeat in list(self.heartbeat_intervals.items()):
                    if current_time - last_heartbeat >= 30:
                        if self.current_lock_holder and self.current_lock_holder == client_id:
                            self.current_lock_holder = None
                            event = f"Lock automatically released due to timeout for client: {client_id}"
                            log_event(event,self.current_address[-1])
                            self.log_event_to_backup(event)

                            print(f"Lock automatically released due to timeout for client: {client_id}")

                            if self.waiting_queue:
                                next_client_id, _ = self.waiting_queue.popleft()
                                self.current_lock_holder = next_client_id
                                self.heartbeat_intervals[next_client_id] = time.time()
                                event = f"Lock granted to next client in queue: {next_client_id}"
                                log_event(event,self.current_address[-1])
                                self.log_event_to_backup(event)

                        del self.heartbeat_intervals[client_id]
                        
    def getCurrent_lock_holder(self,request,context):
        # Retrieve the current lock holder's client ID
        holder = lock_pb2.current_lock_holder(client_id=self.current_lock_holder)
        print(f"current lock holder: {self.current_lock_holder}")
        return lock_pb2.Response(status=lock_pb2.Status.SUCCESS, current_lock_holder=holder)


    def file_append_backup(self,request,context):
        # Handle file append requests on a backup server
        if self.role != BACKUP_SERVER:
            return
        filename = request.filename
        content = request.content.decode()
        is_error = False
        try:
            # Perform the file append on the backup servers
            print(f"Appending to file \n : {filename+self.current_address[-1]}")
            print(f"Append for backup")
            with open(filename+f"_{self.current_address[-1]}.txt", 'a') as file:
                file.write(f" {content}")
           
            if is_error:
                #TO DO : retry replication for some time else timeout
                return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
            else:
                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)

        except FileNotFoundError:
            is_error = True
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)

    def file_append(self, request, context):
        # Append data to a file on the primary server and replicate to backups
        client_id = request.client_id
        request_id = request.request_id
        filename = request.filename
        content = request.content.decode()
        is_error = False

        with self.lock:
            if is_duplicate_request(request_id):
                if request_id not in self.append_request_cache:
                    if is_error:
                        is_error = False
                        return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)   
                    return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
                elif request_id in self.completed_operations_cache:
                    return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
                else:
                    return lock_pb2.Response(status=lock_pb2.Status.DUPLICATE_ERROR)

        mark_request_processed(request_id)
        self.append_request_cache[request_id] = True

        try:
            # Replicate the append operation to backup servers synchronously
            if self.role == PRIMARY_SERVER:
                replication_success = self.replicate_append_to_backups(filename, request.content)
                if not replication_success:
                    is_error = True
            
            if(is_error):
                print("Failed to backup data")

            # Perform the file append on the primary server
            print(f"Appending to file \n : {filename+self.current_address[-1]}")
            with open(filename+f"_{self.current_address[-1]}.txt", 'a') as file:
                file.write(f" {content}")
                
            self.completed_operations_cache[request_id] = True
            print(f"File {filename} appended with content: '{content}' by client {client_id}")

            # Cleanup request cache
            self.cleanup_cache(request_id)

            if is_error:
                # Retry replication or handle the error accordingly
                return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
            else:
                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)

        except FileNotFoundError:
            is_error = True
            self.cleanup_cache(request_id)
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
        
    def Ping(self, request, context):
        response = lock_pb2.PingResponse()
        try:
            # Perform any basic checks, such as server readiness
            response.status = "OK"  # Return "OK" if the server is healthy
        except Exception as e:
            print(f"Error during Ping: {e}")
            response.status = "ERROR"  # Return "ERROR" if something is wrong
        return response
        
    def replicate_append_to_backups(self, filename, content):
        # Replicate file append operation to all active backup servers
        success = True
        for server, server_stub in self.active_backups_server.items():
            if server == self.current_address:
                continue
            result = self.replicate_append_to_backup(server,server_stub, filename, content)
            if not result:
                success = False
        return success

    def replicate_append_to_backup(self,server, server_stub, filename, content):
        # Replicate file append to a single backup server
        try:
           response = server_stub.file_append_backup(lock_pb2.FileAppendBackup(filename=filename, content=content))
           print(f"Replicated to: {server}")
           return response.status == lock_pb2.Status.SUCCESS
        except grpc.RpcError as e:
            print(f"Failed to replicate to {e.details()}, as it is unavailable")
            return False

    def sync_each_file(self, server_stub, filename, content):
        # Send a single file's content to the backup server
        try:
            response = server_stub.sync_file(lock_pb2.FileAppendBackup(filename=filename, content=content))
            if response.status != lock_pb2.Status.SUCCESS:
                print(f"Failed to sync {filename} with backup.")
        except grpc.RpcError as e:
            print(f"Error syncing {filename} with backup: {e.details()}")


    def sync_file(self, request, context):
        # Sync a single file with a backup server
        filename = request.filename +"_"+ self.current_address[-1]+".txt"
        content = request.content.decode()
        try:
            with open(filename,"w") as sync_file:
                sync_file.write(content)
                sync_file.close()
                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
            print(f"File {filename} synced")
        except grpc.RpcError:
            print(f"Failed to synced {filename}")
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)

    def sync_backup_files(self, backup_server, server_stub):
        # Sync all files (file_0 to file_99) with the backup server
        for i in range(100):
            filename = f"Server/Files/file_{i}_{self.current_address[-1]}.txt"
            print(f"Syncing all files with {backup_server}")
            if os.path.exists(filename):
                with open(filename, "rb") as file:
                    content = file.read()
                    self.sync_each_file(server_stub, filename[:-6], content)
            print(f"Sync successful for all files with {backup_server}")

    def cleanup_cache(self, request_id):
        # Clean up cache entries for a specific request ID
        with self.lock:
            if request_id in self.append_request_cache:
                del self.append_request_cache[request_id]

    def log_event_to_backup(self, event):
        # Log an event to all active backup servers
        for backup_address, backup_stub in self.active_backups_server.items():
            if backup_address == self.current_address:
                continue
            try:
                backup_stub.log_event_primary(lock_pb2.Log(event=event.encode()))
                print(f"Log sent to backup at {backup_address}")
            except grpc.RpcError as e:
                print(f"Failed to send logs to backup at {e.details()}")
                return False

    def log_event_primary(self,request,context):
        # Receive and log events from the primary server
        content = request.event.decode()
        print("Received logs from backup")
        try:
            log_event(content,self.current_address[-1])  
            return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
        except grpc.RpcError:
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
    
    def sync_log(self,request,context):
        # Sync log data from the primary server
        content = request.event.decode()
        print("Received logs from backup")
        try:
            write_logs(content, self.current_address[-1])
            return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
        except grpc.RpcError:
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
        
    def sync_logs_with_backup(self,backup_server,server_stub):
        content = get_log_content(self.current_address[-1]).encode()
        try:
            server_stub.sync_log(lock_pb2.Log(event = content))
            print(f"{backup_server} was synced up with primary")
        except grpc.RpcError as e:
            print(f"Fail to sync {e.details()} up with primary")
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
        
    def server_close(self):
        # Gracefully shut down the server
        print("Initiating server shutdown...")
        self.running = False

        with self.lock:
            if self.current_lock_holder is not None:
                event = f"Lock automatically released during shutdown for client: {self.current_lock_holder}"
                log_event(event, self.current_address[-1])
                self.current_lock_holder = None
            while self.waiting_queue:
                client_id, _ = self.waiting_queue.popleft()
                print(f"Removed client {client_id} from waiting queue during shutdown.")
            self.heartbeat_intervals.clear()
            print("Cleared all heartbeat intervals.")
        for backup_address, stub in self.active_backups_server.items():
            try:
                stub.server_shutdown(lock_pb2.Empty())
                print(f"Notified backup server at {backup_address} about shutdown.")
            except grpc.RpcError as e:
                print(f"Failed to notify backup server at {backup_address}: {e.details()}")

        sys.exit("Server shutdown complete. Exiting...")

    def server_shutdown(self, request, context):
        # Handle shutdown request from another server
        print(f"Received shutdown notification for server {self.server_id}.")
        self.server_close()
        return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)

def serve(server_id,peers,role):
    # Start the gRPC server and register the LockService
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lock_service = LockServiceServicer(server_id, peers,role=role)
    lock_pb2_grpc.add_LockServiceServicer_to_server(lock_service, server)
    server.add_insecure_port(f'[::]:{server_id}')
    server.start()
    lock_service.current_address = f"localhost:{server_id}"
    if(role == PRIMARY_SERVER):
        threading.Thread(target=lock_service.check_heartbeats, daemon=True).start()
    print(f"{role.capitalize()} server started at: {server_id}")
    
    # Handle server termination signals (e.g., SIGINT, SIGTERM)
    def signal_handler(sig, frame):
        print("Signal received, shutting down server...")
        lock_service.server_close()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    print("Server is running. Press Ctrl+C to stop.")
    server.wait_for_termination()
    print("Server stopped.")

if __name__ == '__main__':
    # Set role as "primary" or "backup" here. Defaulting to primary for demonstration.
    server_id = input("Enter server id  ")
    role = input("Enter the role of the server  ")
    peers = ['50051','50052','50053','50054']

    if(server_id == '1') :
        server_id = '50051'
    elif(server_id == '2') :
        server_id = '50052'
    elif(server_id == '3') :
        server_id = '50053'
    elif(server_id == '4') :
        server_id = '50054'

    serve(server_id,peers,role)
