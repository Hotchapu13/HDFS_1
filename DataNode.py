import socket
import threading
import json
import os
import time

# Configuration
NAMENODE_HOST = '192.168.164.58'  # Replace with the NameNode's IP
NAMENODE_PORT = 5000
DATANODE_HOST = '0.0.0.0'  # Listen on all interfaces
DATANODE_PORT = 5001  # Port for this DataNode
STORAGE_DIR = "datanode_storage"  # Directory to store file blocks
HEARTBEAT_INTERVAL = 10  # Send heartbeat every 10 seconds

# Ensure storage directory exists
os.makedirs(STORAGE_DIR, exist_ok=True)

def get_local_ip():
    """Get the local IP address of the DataNode."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))  # Connect to a public DNS server
        return s.getsockname()[0]

def send_heartbeat():
    """Send periodic heartbeats to the NameNode."""
    datanode_ip = get_local_ip()  # Get the actual IP address
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((NAMENODE_HOST, NAMENODE_PORT))
                heartbeat_message = {
                    "action": "heartbeat",
                    "datanode_host": datanode_ip,  # Use the actual IP address
                    "datanode_port": DATANODE_PORT
                }
                data = json.dumps(heartbeat_message).encode()
                s.sendall(len(data).to_bytes(4, byteorder='big'))
                s.sendall(data)
        except Exception as e:
            print(f"[ERROR] Failed to send heartbeat: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

def process_message(message, conn):
    if message["message_type"] == "file_chunk":
        chunk_id = message["chunk_id"]
        filename = os.path.join(STORAGE_DIR, chunk_id)
        filesize = message["chunk_size"]
        
        # Receive the file data
        with open(filename, 'wb') as f:
            bytes_received = 0
            while bytes_received < filesize:
                chunk = conn.recv(min(4096, filesize - bytes_received))
                if not chunk:
                    raise ConnectionError("Client disconnected during file upload")
                f.write(chunk)
                bytes_received += len(chunk)
        
        print(f"[UPLOAD] Chunk {chunk_id} received and stored.")
        return {"status": "success", "message": f"Chunk {chunk_id} stored successfully"}
    
    else:
        print(f"[DEBUG] Unknown message type: {message['message_type']}")
        return {"status": "error", "message": "Unknown message type"}

def handle_client(conn, addr):
    print(f"[DataNode] Connected by {addr}")
    try:
        while True:
            # Receive the length of the incoming message
            length_bytes = conn.recv(4)
            if not length_bytes:
                break
            message_length = int.from_bytes(length_bytes, byteorder='big')
            
            # Receive the actual message
            data = conn.recv(message_length)
            if not data:
                break
            message = json.loads(data.decode('utf-8'))
            print(f"[DEBUG] Received message: {message}")
            
            # Process the message
            response = process_message(message, conn)
            
            # Send the response
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big') + response_data)
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        conn.close()
        print(f"[DataNode] Disconnected from {addr}")

def start_server():
    """Start the DataNode server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((DATANODE_HOST, DATANODE_PORT))
        s.listen()
        print(f"[DataNode] Listening on {DATANODE_HOST}:{DATANODE_PORT}")
        
        # Start heartbeat thread
        threading.Thread(target=send_heartbeat, daemon=True).start()
        
        while True:
            conn, addr = s.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()

if __name__ == "__main__":
    start_server()