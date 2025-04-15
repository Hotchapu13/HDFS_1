import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import socket, json
import threading
import time
import math

NAMENODE_HOST = '192.168.164.58'  # or the IP of the NameNode
NAMENODE_PORT = 5000

class FileStorageClientGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Distributed File Storage Client")
        self.root.geometry("800x500")

        self.selected_path = tk.StringVar()
        self.upload_history = []

        # Set up UI
        self.create_widgets()
        
        # Set up status indicator
        self.status_var = tk.StringVar(value="Ready")
        self.status_label = tk.Label(self.root, textvariable=self.status_var)
        self.status_label.pack(side=tk.BOTTOM, pady=5)
        
        # Set up cleanup when window closes
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_widgets(self):
        title = tk.Label(self.root, text="Cloud File Storage", font=("Helvetica", 16, "bold"))
        title.pack(pady=10)

        # Select File or Folder
        select_frame = tk.Frame(self.root)
        select_frame.pack(pady=10)

        path_entry = tk.Entry(select_frame, textvariable=self.selected_path, width=50)
        path_entry.pack(side=tk.LEFT, padx=5)

        file_btn = tk.Button(select_frame, text="Select File", command=self.select_file)
        file_btn.pack(side=tk.LEFT, padx=5)

        folder_btn = tk.Button(select_frame, text="Select Folder", command=self.select_folder)
        folder_btn.pack(side=tk.LEFT, padx=5)

        # Upload and Download Buttons
        action_frame = tk.Frame(self.root)
        action_frame.pack(pady=10)

        upload_btn = tk.Button(action_frame, text="Upload", width=15, command=self.upload)
        upload_btn.pack(side=tk.LEFT, padx=10)

        download_btn = tk.Button(action_frame, text="Download", width=15, command=self.download)
        download_btn.pack(side=tk.LEFT, padx=10)

        # Upload History
        history_label = tk.Label(self.root, text="Upload History", font=("Helvetica", 12, "bold"))
        history_label.pack(pady=5)

        self.history_tree = ttk.Treeview(self.root, columns=("Name", "Type", "Size (KB)"), show='headings')
        self.history_tree.heading("Name", text="Name")
        self.history_tree.heading("Type", text="Type")
        self.history_tree.heading("Size (KB)", text="Size (KB)")
        self.history_tree.pack(fill=tk.BOTH, expand=True, padx=20)

    def select_file(self):
        file_path = filedialog.askopenfilename()
        if file_path:
            self.selected_path.set(file_path)

    def select_folder(self):
        folder_path = filedialog.askdirectory()
        if folder_path:
            self.selected_path.set(folder_path)

    def send_request(self, message):
        """Send a request to the NameNode and get response"""
        try:
            # Update status
            self.status_var.set(f"Connecting to NameNode...")
            self.root.update_idletasks()
            
            # Create a new connection
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)  # 10 second timeout
                s.connect((NAMENODE_HOST, NAMENODE_PORT))
                
                # Send the message
                data = json.dumps(message).encode()
                # Send length prefix (4 bytes)
                s.sendall(len(data).to_bytes(4, byteorder='big'))
                # Then send the message
                s.sendall(data)
                
                # Receive response length
                length_bytes = s.recv(4)
                if not length_bytes:
                    raise ConnectionError("Connection closed by server")
                    
                length = int.from_bytes(length_bytes, byteorder='big')
                
                # Receive response data
                response_data = b''
                remaining = length
                while remaining > 0:
                    chunk = s.recv(min(4096, remaining))
                    if not chunk:
                        raise ConnectionError("Connection closed by server")
                    response_data += chunk
                    remaining -= len(chunk)
                
                # Update status
                self.status_var.set("Ready")
                
                return json.loads(response_data.decode())
                
        except Exception as e:
            self.status_var.set(f"Error: {str(e)}")
            messagebox.showerror("Connection Error", f"Failed to communicate with NameNode: {str(e)}")
            return None

    def upload(self):
        path = self.selected_path.get()
        if not path:
            messagebox.showerror("Error", "No file or folder selected.")
            return

        name = os.path.basename(path)
        
        if os.path.isfile(path):
            file_size = os.path.getsize(path)
            chunk_size = 64 * 1024 * 1024  # 64 MB
            num_chunks = math.ceil(file_size / chunk_size)
            
            # Show progress dialog
            progress_window = tk.Toplevel(self.root)
            progress_window.title("Uploading")
            progress_window.geometry("300x150")
            tk.Label(progress_window, text=f"Uploading {name}...").pack(pady=10)
            progress = ttk.Progressbar(progress_window, orient="horizontal", length=250, mode="determinate")
            progress.pack(pady=10)
            status_label = tk.Label(progress_window, text="Requesting upload...")
            status_label.pack(pady=5)
            
            # Run upload in background thread
            def do_upload():
                try:
                    # Step 1: Request upload from NameNode
                    response = self.send_request({
                        "action": "upload",
                        "name": name,
                        "filesize": file_size,
                        "num_chunks": num_chunks
                    })
                    
                    print(f"[DEBUG] NameNode response: {response}")  # Add this line

                    if not response:
                        progress_window.destroy()
                        return
                    
                    if response.get("status") != "ok":
                        progress_window.destroy()
                        messagebox.showerror("Upload Error", response.get("message", "Unknown error"))
                        return
                    
                    chunk_allocations = response.get("chunk_allocations", [])
                    if not chunk_allocations or len(chunk_allocations) != num_chunks:
                        progress_window.destroy()
                        messagebox.showerror("Upload Error", "Invalid chunk allocation from NameNode")
                        return
                    
                    # Step 2: Get DataNode info for each chunk
                    chunk_allocations = response.get("chunk_allocations", [])
                    if not chunk_allocations or len(chunk_allocations) != num_chunks:
                        progress_window.destroy()
                        messagebox.showerror("Upload Error", "Invalid chunk allocation from NameNode")
                        return
                    
                    # Step 3: Upload each chunk to the assigned DataNodes
                    with open(path, 'rb') as f:
                        for i, allocation in enumerate(chunk_allocations):
                            chunk_data = f.read(chunk_size)
                            chunk_id = allocation["chunk_id"]
                            datanodes = allocation["datanodes"]
                            
                            # Upload to primary DataNode
                            primary = datanodes[0]
                            try:
                                print(f"[DEBUG] Connecting to DataNode: {primary['host']}:{primary['port']}")
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                    s.connect((primary['host'], primary['port']))
                                    # Send metadata
                                    metadata = json.dumps({
                                        "message_type": "file_chunk",
                                        "chunk_id": chunk_id,
                                        "filename": name,
                                        "chunk_index": i,
                                        "chunk_size": len(chunk_data)
                                    }).encode()
                                    
                                    s.sendall(len(metadata).to_bytes(4, byteorder='big'))  # Send metadata length
                                    s.sendall(metadata)  # Send metadata
                                    s.sendall(chunk_data)  # Send chunk data
                                    
                                    # Get confirmation
                                    resp_len = int.from_bytes(s.recv(4), byteorder='big')
                                    resp = s.recv(resp_len).decode()
                                    
                                    if "success" not in resp.lower():
                                        raise Exception(f"Chunk {i} upload failed: {resp}")
                            
                            except Exception as e:
                                print(f"[ERROR] Failed to upload chunk {i} to DataNode: {e}")
                                raise
                            
                            # Update progress
                            progress_window.after(0, lambda: progress.config(value=((i + 1) / num_chunks) * 100))
                    
                    # Step 4: Confirm upload with NameNode
                    confirmation = self.send_request({
                        "action": "upload_complete",
                        "filename": name,
                        "filesize": file_size
                    })
                    
                    print(f"[DEBUG] Upload confirmation response: {confirmation}")

                    # Close progress dialog
                    progress_window.destroy()
                    
                    if confirmation and confirmation.get("status") == "ok":
                        # Add to history list
                        self.upload_history.append((name, "File", file_size // 1024))
                        self.history_tree.insert("", tk.END, values=(name, "File", file_size // 1024))
                        messagebox.showinfo("Upload", f"'{name}' uploaded successfully.")
                    else:
                        messagebox.showerror("Upload Error", "Failed to confirm upload with NameNode")
                
                except Exception as e:
                    progress_window.destroy()
                    messagebox.showerror("Upload Error", f"An error occurred: {str(e)}")
            
            # Start upload thread
            threading.Thread(target=do_upload, daemon=True).start()
            
        else:
            # Handle folder upload
            messagebox.showinfo("Not Implemented", "Folder upload not fully implemented yet.")

    def download(self):
        selected = self.history_tree.focus()
        if not selected:
            messagebox.showerror("Error", "No file selected to download.")
            return

        file_info = self.history_tree.item(selected)["values"]
        filename = file_info[0]

        # Ask user where to save the file
        save_path = filedialog.asksaveasfilename(
            title="Save As",
            initialfile=filename
        )
        
        if not save_path:  # User cancelled
            return
            
        # Show progress dialog
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Downloading")
        progress_window.geometry("300x150")
        tk.Label(progress_window, text=f"Downloading {filename}...").pack(pady=10)
        progress = ttk.Progressbar(progress_window, orient="horizontal", length=250, mode="determinate")
        progress.pack(pady=10)
        status_label = tk.Label(progress_window, text="Requesting file location...")
        status_label.pack(pady=5)
        
        # Run download in background thread
        def do_download():
            try:
                # Step 1: Request file metadata from NameNode
                response = self.send_request({
                    "action": "download",
                    "name": filename
                })
                
                if not response:
                    progress_window.destroy()
                    return
                    
                if response.get("status") != "ok":
                    progress_window.destroy()
                    messagebox.showerror("Download Error", response.get("message", "File not found"))
                    return
                
                # Step 2: Get chunk information
                chunks = response.get("chunks", [])
                if not chunks:
                    progress_window.destroy()
                    messagebox.showerror("Download Error", "No chunk information available")
                    return
                
                # Step 3: Download each chunk and reconstruct the file
                with open(save_path, 'wb') as f:
                    for i, chunk in enumerate(chunks):
                        chunk_id = chunk["chunk_id"]
                        datanodes = chunk["datanodes"]
                        
                        # Try downloading from the first available DataNode
                        success = False
                        for datanode in datanodes:
                            try:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                    s.connect((datanode["host"], datanode["port"]))
                                    
                                    # Request the chunk
                                    request = json.dumps({
                                        "message_type": "get_file",
                                        "chunk_id": chunk_id
                                    }).encode()
                                    
                                    s.sendall(len(request).to_bytes(4, byteorder='big'))  # Send request length
                                    s.sendall(request)  # Send request
                                    
                                    # Get chunk size
                                    size_bytes = s.recv(8)
                                    chunk_size = int.from_bytes(size_bytes, byteorder='big')
                                    
                                    # Download chunk data
                                    bytes_received = 0
                                    while bytes_received < chunk_size:
                                        chunk_data = s.recv(min(8192, chunk_size - bytes_received))
                                        if not chunk_data:
                                            raise ConnectionError("DataNode disconnected during chunk download")
                                        f.write(chunk_data)
                                        bytes_received += len(chunk_data)
                                    
                                    success = True
                                    break  # Exit loop if chunk is successfully downloaded
                            except Exception as e:
                                print(f"[ERROR] Failed to download chunk {chunk_id} from {datanode['host']}:{datanode['port']}: {e}")
                        
                        if not success:
                            progress_window.destroy()
                            messagebox.showerror("Download Error", f"Failed to download chunk {chunk_id}")
                            return
                        
                        # Update progress
                        progress_window.after(0, lambda: progress.config(value=((i + 1) / len(chunks)) * 100))
                
                # Close progress dialog
                progress_window.destroy()
                messagebox.showinfo("Download Complete", f"'{filename}' downloaded successfully.")
            
            except Exception as e:
                progress_window.destroy()
                messagebox.showerror("Download Error", f"An error occurred: {str(e)}")
        
        # Start download thread
        threading.Thread(target=do_download, daemon=True).start()

    def get_folder_size_kb(self, folder):
        total_size = 0
        for dirpath, _, filenames in os.walk(folder):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.isfile(fp):
                    total_size += os.path.getsize(fp)
        return total_size // 1024
    
    def on_closing(self):
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = FileStorageClientGUI(root)
    root.mainloop()
