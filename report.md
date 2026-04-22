# Distributed Key-Value Store Project Report

## 1. Project Overview
This project is a Python-based decentralized, distributed key-value store. It uses Flask to expose a peer-to-peer network interface that allows multiple independent nodes to connect, form a network, and synchronize state in the form of shared variables. The primary goal is to maintain a consistent shared state across multiple machines while handling network partitions and node failures.

## 2. Architecture & Design

### 2.1 Node Structure
Each node acts as both a client and a server. When a node starts, it determines its local IP address and listens on a specified port using a Flask web server. Nodes maintain a list of `KNOWN_HOSTS` in memory, which acts as their routing table. 

### 2.2 Shared Variables & Ownership
The core data structure is the `SharedValue` class. Each variable consists of:
- **Value**: The actual string data.
- **Modification Timestamp**: A float indicating when the variable was last changed.
- **Owner ID**: The unique identifier of the node that originally created or last successfully acquired ownership of the variable.

If a node wants to update a variable it does not own, it must first send a request (`/request_access/`) to the current owner. If the owner approves, the ownership is transferred, and the update proceeds. Conflict resolution is handled gracefully by comparing modification timestamps—the update with the most recent timestamp always wins.

### 2.3 Persistence (Database)
To ensure data is not lost when a node crashes or restarts, every shared variable is persisted to a local SQLite database (`DISTRIBUTED_CONFIG.sqlite`). On startup, a node reads its SQLite file and populates its in-memory `STORED_VARIABLES` cache.

## 3. Network Propagation & Synchronization

The system uses a next-host propagation model to disseminate information (gossip protocol flavor). Instead of broadcasting every update to every single node simultaneously, updates are passed to the `next_host` in a sorted ring topology.

### 3.1 Network Joining
When a new node joins an existing network:
1. It connects to a known bootstrap node.
2. It downloads the list of active hosts (`/internal/known_hosts/`).
3. It integrates itself into the network and triggers a host list propagation.
4. It synchronizes its local variables with the network, ensuring its state is up-to-date.

### 3.2 Fault Tolerance
The system is designed to handle missing or disconnected nodes:
- **Pings & Heartbeats**: Nodes continuously check if the `next_host` is alive using a `/ping/` endpoint.
- **Lost Hosts Reconnection**: If a node goes offline, it is moved to `LOST_HOSTS`. A background thread (`try_to_connect_to_lost_hosts`) periodically attempts to reconnect to these nodes.
- **Delayed Updates Queue**: If a node is disconnected but receives local updates, those updates are placed in a `DELAYED_UPDATE_QUEUE`. Once the node regains connection and ownership is verified, the queued updates are processed and propagated.

## 4. API Endpoints

### 4.1 Client Endpoints (Public)
These endpoints are intended for users interacting with the distributed store:
- `GET /variable/<name>/`: Retrieves the JSON value of the requested variable.
- `POST /variable/<name>/update/`: Updates the value of the variable. The request body should contain the new raw string value.

### 4.2 Internal Endpoints (Peer-to-Peer)
These endpoints are used exclusively for node-to-node communication:
- `GET /ping/`: Health check endpoint.
- `POST /internal/join/<connecting_id>/`: Used by a new node to join the network and sync data.
- `POST /internal/new_host/<connecting_id>/`: Propagates the discovery of a new node to the network.
- `GET /internal/delete_host/<delete_id>/`: Propagates the removal of a dead node.
- `GET /internal/variable/<name>/`: Internal retrieval of a variable's full state (including timestamp and owner).
- `POST /internal/variable/<name>/update/`: Propagates a variable update across the network.
- `GET /internal/variable/<name>/request_access/<owner_id>/`: Requests ownership of a variable.

## 5. How to Run

### Prerequisites
- Python 3.11+
- Flask (`pip install Flask`)
- Requests (`pip install requests`)

### Starting a Node
To start the first node, create a configuration file (e.g., `config.ini`) containing the port and database details, then run:
```bash
python python/src/main.py -c config.ini
```

### Joining an Existing Network
To start a new node and connect it to an existing network, provide the host and port of the existing node:
```bash
python python/src/main.py -c config_node2.ini -h <existing_node_ip> -p <existing_node_port>
```

## 6. Experience and Skills Gained

Developing this distributed key-value store provided practical experience in several core areas of software engineering and distributed systems:

- **Distributed Systems Concepts**: Gained hands-on understanding of peer-to-peer network topologies, gossip-like propagation protocols, and eventual consistency.
- **Concurrency & Multithreading**: Improved proficiency in Python's `threading` module, particularly in managing race conditions using `Lock()` to protect shared resources like `STORED_VARIABLES` and `DELAYED_UPDATE_QUEUE`.
- **Fault Tolerance & Resilience**: Designed mechanisms to handle network partitions and node failures, implementing background recovery threads and offline queues.
- **Conflict Resolution**: Implemented custom logical timestamp-based conflict resolution and ownership transfer protocols to ensure data integrity across multiple writers.
- **Network Programming (HTTP/REST)**: Extensively used Flask to build both public-facing API endpoints and internal node-to-node communication channels over HTTP.
- **Database Persistence**: Integrated SQLite using best-practice context managers to ensure safe transactions and data persistence across application restarts.
- **Code Refactoring & Quality**: Refactored the initial implementation to adhere to best practices, such as replacing fragile bare exceptions with specific `requests.RequestException` blocks, standardizing output using Python's `logging` module, and using `argparse` for robust command-line configuration.
