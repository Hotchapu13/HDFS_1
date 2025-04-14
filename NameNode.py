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
            data = b''
            while len(data) < message_length:
                chunk = conn.recv(min(4096, message_length - len(data)))
                if not chunk:
                    raise ConnectionError("Client disconnected before sending full message")
                data += chunk
            message = json.loads(data.decode('utf-8'))
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
                    chunk_allocations.append({
                        "chunk_id": chunk_id,
                        "datanodes": datanodes
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
            
            # Send response
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big') + response_data)
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        conn.close()
        print(f"[NameNode] Disconnected from {addr}")

def allocate_datanodes():
    """Allocate DataNodes for a chunk (replication factor = 3)."""
    datanodes = list(DATANODE_STATUS.keys())  # List of available DataNodes
    return [{"host": dn["host"], "port": dn["port"]} for dn in datanodes[:3]]  # Select 3 DataNodes

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
