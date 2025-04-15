# namenode.py
import socket
import threading
import json
import os
import time

FILE_METADATA = {}  # File metadata
BLOCK_METADATA = {}  # Block metadata
DATANODE_STATUS = {}  # Datanode health status
METADATA_FILE = "namenode_metadata.json"
HOST = '0.0.0.0'
PORT = 5000
metadata_lock = threading.Lock()

def load_metadata():
    global FILE_METADATA
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            FILE_METADATA = json.load(f)
            print(f"[NameNode] Metadata loaded: {len(FILE_METADATA)} entries")

def save_metadata():
    with metadata_lock:
        with open(METADATA_FILE, "w") as f:
            json.dump(FILE_METADATA, f, indent=2)
            print("[NameNode] Metadata saved.")

def handle_client(conn, addr):
    print(f"[NameNode] Connected by {addr}")
    try:
        while True:
            length_bytes = conn.recv(4)
            if not length_bytes:
                break
            message_length = int.from_bytes(length_bytes, byteorder='big')
            data = conn.recv(message_length)
            if not data:
                break
            message = json.loads(data.decode('utf-8'))
            print(f"[DEBUG] Received message: {message}")
            
            # Process the message
            response = process_message(message)
            
            # Send the response
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big') + response_data)
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        conn.close()
        print(f"[NameNode] Disconnected from {addr}")

def process_message(message):
    response = {}

    # Handle upload request
    if message["action"] == "upload":
        filename = message["name"]
        filesize = message["filesize"]
        num_chunks = message["num_chunks"]
        
        # Allocate DataNodes for each chunk
        chunk_allocations = []
        for i in range(num_chunks):
            chunk_id = f"{filename}_chunk_{i}"
            datanodes = allocate_datanodes()  # Function to select DataNodes
            if not datanodes:
                response = {"status": "error", "message": "Not enough DataNodes available"}
                return response
            chunk_allocations.append({
                "chunk_id": chunk_id,
                "datanodes": [{"host": dn["host"], "port": dn["port"]} for dn in datanodes]
            })
        
        # Store file metadata
        with metadata_lock:
            FILE_METADATA[filename] = {
                "size": filesize,
                "chunks": chunk_allocations
            }
        save_metadata()
        
        response = {
            "status": "ok",
            "chunk_allocations": chunk_allocations
        }
        print(f"[DEBUG] Chunk Allocations Sent to Client: {chunk_allocations}")

    # Handle download request
    if message["action"] == "download":
        filename = message["name"]
        with metadata_lock:
            if filename in FILE_METADATA:
                response = {
                    "status": "ok",
                    "chunks": FILE_METADATA[filename]["chunks"]
                }
            else:
                response = {"status": "error", "message": "File not found"}

    # Handle upload complete request
    if message["action"] == "upload_complete":
        filename = message["filename"]
        filesize = message["filesize"]
        
        print(f"[DEBUG] Received upload_complete request: {message}")
        print(f"[DEBUG] Current FILE_METADATA: {FILE_METADATA}")
        
        with metadata_lock:
            if filename in FILE_METADATA:
                FILE_METADATA[filename]["status"] = "complete"
                save_metadata()
                response = {"status": "ok", "message": f"Upload of {filename} confirmed"}
            else:
                response = {"status": "error", "message": "File metadata not found"}

    # Handle heartbeat
    if message["action"] == "heartbeat":
        datanode_host = message["datanode_host"]
        datanode_port = message["datanode_port"]
        datanode_id = f"{datanode_host}:{datanode_port}"
        DATANODE_STATUS[datanode_id] = {
            "host": datanode_host,
            "port": datanode_port,
            "last_heartbeat": time.time()
        }
        print(f"[DEBUG] Heartbeat received from {datanode_id}")
        response = {"status": "success"}
    
    return response

def allocate_datanodes():
    """Allocate DataNodes for a chunk (replication factor = 2)."""
    datanodes = list(DATANODE_STATUS.values())  # Get the list of available DataNodes
    if len(datanodes) < 2:
        print("[ERROR] Not enough DataNodes available for replication")
        return []  # Return an empty list if there aren't enough DataNodes
    print(f"[DEBUG] Registered DataNodes: {DATANODE_STATUS}")
    return datanodes[:2]  # Select the first 3 DataNodes

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"[NameNode] Listening on {HOST}:{PORT}")
        while True:
            conn, addr = s.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()

if __name__ == "__main__":
    load_metadata()
    start_server()
