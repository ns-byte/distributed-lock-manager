# distributed-lock-manager
This Distributed System is make using Exclusion lock mechanism along with multi-threading and proto

# Project Specification Part-1
1. A Simple Lock Server
2. A Client library

=> Read method details from the readme_description from the assigment. 

=> Assign a point from below to yourself. Explain in this readme about the code you are going to do in the next section below -> commit the explanation -> start coding.

=> First both init needs to be created and tested to check the working of our server.

=> After checking init we can start working seperately by putting name infront of the task we are doing in any order.

=> Create a new branch for all new work or a same branch (do not merge to main directly). Let's discuss the code, explain and merge by the end of the day, everyday.

=> Not able to understand something, need help or wanna switch task, call/message on the group.

=> Many of the following method,  needs some design discussions or thinking, whatever you think is write note it down (Eg: Data structure used, method used,) We need to add this to the design report later.

=> Do not think of efficiency in the starting. This is part 1 keep it as simple as possible.

=> Feel free to add anything in the below points, a sub point or extension or explanation as you like, please commit readme first before working. Ping in the group once something change. 

=> Use the utils file to store any method that is not the original method of the assignment specification. 

=> Run following command before generating proto using the cmmand given in assignement:
**pip install grpcio grpcio-tools**

=> The command given in assignment have to run on root directory. It should be modified as following since I have updated the directory structure:
**python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. Proto/lock.proto**

=> Update proto files as required for the communication between server and client. That is you can add or update any field in proto please make sure to run proto command each time that is done.


## To Run code
    On the root directory:
    Terminal 1:
    python -m Server.server.py
    Terminal 2:
    python -m Client.client.py

## Client Library
   Client library main file is : Client/client.py
   1. Create functional structure of client.py 
   2. Create the proto files, using the command given 
   3. Create init method.
   4. Create Acqruire lock method 
   5. Create Release lock method 
   6. Create Append file method
       a. It should filter the file client is modifying 
       b. Perform append operation on it.
   7. Create a prompt that takes input from client with the file name that needs to be updated.
   8. Create close method.
   9. Add retry-mechanism - Aditi
   10. Send Heart-beat - Aditi
   11. Add Data consistency - Aditi
  
## Lock Server
   Server's main file is: Server/server.py. 
   For server implementation refer to the spinlock implementation given in assigment readme and assignment folder.
   1. Create the proto files, using the command given - 
   2. Add 100 files to Server folder. - 
   3. Create functional structure of server.py - 
   4. Create the init method - 
   5. Create structure for multithreaded environment
   6. Create the lock_acquire method - 
   7.  Create the lock_release method - 
   8. Create the append_file method. - 
   9. Create close method
   10. Add fault-tolerance for server crash - 
   11. Add Heart-beat mechanism - 


# Design Pointers
Mention every method you write what it does what it handles, which all files the code is included.

## Fault Taulerance 

1. Network Failure: Packet Loss (Retry Mechanism)
Client-Side Change: Implement a retry mechanism in the client to handle packet loss, with a maximum retry count.
2. Network Failure: Duplicated Requests (Idempotent Handling)
Server-Side Change: Track each request by a unique ID to ensure idempotent operations.
3. Client Crash (Heartbeat Mechanism)
Client-Side Change: Send periodic heartbeats to the server while holding the lock.
Server-Side Change: Use heartbeats to detect client crashes and release locks after a timeout.
4. Client Crash with Data Consistency (Versioned Lock Ownership)
Server-Side Change: Add a version counter to ensure only the latest client with the lock can modify or release it.
5. Server Crash and Recovery (Log-Structured File System)
Server-Side Change: Log each lock acquisition, release, and waiting queue state. On restart, the server will replay the log to restore the state.

1. Network Failure: Packet Loss (Retry Mechanism)
    -> We have implemented retry mechanism on lock_acquire and lock_release , to ensure if the packet was lost on a retry it gets delivered to the server.
    -> Lock_Acquire and Lock_Release:
        We have added a retry of 5 seconds buffer before the client retry to check if it is able to acquire lock. 
        We have added a retry buffer of 5+5 which gradually increase till 30 seconds, if the server is unavailable.
2. Duplicate Request: 
    -> We have implemented a unique_id for each client's each request. So that when client tries the retry mechanism, it does not bombard the server with same request.
    -> We have ensured if the request_id to be unique and kept it in set at the server level, each time a request drops in we check if the client has already requested, if it has we simply check if the request if processed , if is processed we send success else we send failure and client tries to request again.
3. Client Crashes:
    -> When client crashes and if it holds a lock, we are making sure that server will automatically release the lock if client hearbeat that was last sent from the client was more the 30 seconds ago. 
    -> This is done in the check_heartbeat function which runs in a seperate thread at the server side.
4. Lock Holder's Crash Recovery:

5. Server Failure:
    **Load Balancing**: While all servers are running, only the primary handles client requests directly. Backups stay in sync by following the primary’s log and do not process client requests unless they become the primary.

    **Heartbeat Mechanism**: Regular heartbeats are exchanged between the primary and backups to detect failures quickly. If a primary server fails, backups notice the absence of heartbeats and initiate a leader election to select a new primary.
