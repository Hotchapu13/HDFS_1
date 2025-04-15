# Distributed File System Implementation

This project implements a distributed file system with a client-server architecture, similar to Hadoop's HDFS (Hadoop Distributed File System). The system consists of three main components: a NameNode, DataNodes, and a User Client with a graphical interface.

## Components

### 1. NameNode (`NameNode.py`)
- Acts as the central metadata manager
- Maintains file system metadata and block locations
- Handles file upload/download requests
- Manages DataNode health through heartbeats
- Implements replication (2x) for data reliability
- Persists metadata to disk for recovery

### 2. DataNode (`DataNode.py`)
- Stores actual file chunks
- Sends periodic heartbeats to NameNode
- Handles file chunk uploads and storage
- Automatically detects and uses local IP address
- Maintains a dedicated storage directory

### 3. User Client (`User.py`)
- Provides a graphical user interface for file operations
- Supports file upload and download
- Shows upload history and file details
- Handles file chunking and distribution
- Implements progress tracking for uploads

## Features

- **File Upload**: Split files into chunks and distribute across DataNodes
- **File Download**: Reconstruct files from distributed chunks
- **Replication**: Each file chunk is stored on multiple DataNodes (2x replication)
- **Fault Tolerance**: Heartbeat mechanism to track DataNode health
- **Metadata Persistence**: NameNode metadata is saved to disk
- **User-Friendly Interface**: GUI for easy file operations

## Configuration

### NameNode Configuration
- Host: `0.0.0.0` (listens on all interfaces)
- Port: `5000`
- Metadata File: `namenode_metadata.json`

### DataNode Configuration
- NameNode Host: `192.168.164.58` (configurable)
- NameNode Port: `5000`
- DataNode Port: `5001`
- Storage Directory: `datanode_storage`
- Heartbeat Interval: 10 seconds

### User Client Configuration
- NameNode Host: `192.168.164.58` (configurable)
- NameNode Port: `5000`

## Usage

1. Start the NameNode:
```bash
python NameNode.py
```

2. Start one or more DataNodes:
```bash
python DataNode.py
```

3. Launch the User Client:
```bash
python User.py
```

## Requirements

- Python 3.x
- tkinter (for GUI)
- Standard Python libraries (socket, threading, json, os)

## Architecture

The system follows a master-slave architecture:
- NameNode (master): Manages metadata and coordinates operations
- DataNodes (slaves): Store actual file data
- User Client: Provides interface for file operations

## File Operations

### Upload Process
1. User selects file through GUI
2. Client requests upload from NameNode
3. NameNode allocates DataNodes for chunks
4. Client splits file and uploads chunks to DataNodes
5. NameNode confirms successful upload

### Download Process
1. User selects file from history
2. Client requests file metadata from NameNode
3. Client downloads chunks from DataNodes
4. Client reconstructs original file

## Notes

- The system currently supports single file uploads
- Folder upload functionality is not fully implemented
- Default chunk size is 64MB
- Replication factor is set to 2
