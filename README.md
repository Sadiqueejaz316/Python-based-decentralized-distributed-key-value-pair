# Python-based Decentralized Distributed Key-Value Store

This project is a decentralized, distributed key-value store built in Python. It uses Flask to expose a peer-to-peer network interface, allowing multiple independent nodes to connect, form a network, and synchronize state in the form of shared variables. The primary goal is to maintain a consistent shared state across multiple machines while handling network partitions and node failures gracefully.

## Architecture & Design

### Node Structure
Each node acts as both a client and a server. When a node starts, it determines its local IP address and listens on a specified port. Nodes maintain a list of `KNOWN_HOSTS` in memory, which acts as their routing table to communicate with peers.

### Shared Variables & Ownership
The core data structure is the `SharedValue` class. Each variable consists of:
- **Value**: The actual string data.
- **Modification Timestamp**: A timestamp indicating when the variable was last changed.
- **Owner ID**: The unique identifier of the node that originally created or last successfully acquired ownership.

If a node wants to update a variable it does not own, it must first send a request (`/request_access/`) to the current owner. If the owner approves, the ownership is transferred. Conflict resolution is handled by comparing modification timestamps—the update with the most recent timestamp always wins.

### Persistence (Database)
To ensure data is not lost when a node crashes or restarts, every shared variable is persisted to a local SQLite database. On startup, a node reads its SQLite file and populates its in-memory cache to resume operations seamlessly.

## Network Propagation & Synchronization

The system uses a next-host propagation model to disseminate information. Instead of broadcasting every update to every single node simultaneously, updates are passed to the `next_host` in a sorted ring topology.

- **Network Joining**: When a new node joins, it connects to a known bootstrap node, downloads the list of active hosts, integrates itself into the network, and synchronizes its local variables.
- **Fault Tolerance**: The system handles disconnected nodes via pings. If a node goes offline, it is moved to `LOST_HOSTS`. A background thread periodically attempts to reconnect. If a disconnected node receives local updates, they are placed in a `DELAYED_UPDATE_QUEUE` to be processed once the connection is restored.

## API Endpoints

### Client Endpoints (Public)
These endpoints are intended for users interacting with the distributed store:
- `GET /variable/<name>/`: Retrieves the JSON value of the requested variable.
- `POST /variable/<name>/update/`: Updates the value of the variable. The request body should contain the new raw string value.

### Internal Endpoints (Peer-to-Peer)
These endpoints are used exclusively for node-to-node communication:
- `GET /ping/`: Health check endpoint.
- `POST /internal/join/<connecting_id>/`: Used by a new node to join the network and sync data.
- `POST /internal/new_host/<connecting_id>/`: Propagates the discovery of a new node to the network.
- `GET /internal/delete_host/<delete_id>/`: Propagates the removal of a dead node.
- `GET /internal/variable/<name>/`: Internal retrieval of a variable's full state.
- `POST /internal/variable/<name>/update/`: Propagates a variable update across the network.
- `GET /internal/variable/<name>/request_access/<owner_id>/`: Requests ownership of a variable.

## How to Run

### Prerequisites
- Python 3.11+
- Install dependencies using the provided requirements file:
```bash
pip install -r requirements.txt
```

### Starting the First Node
To start the first node, create a configuration file (e.g., `config.ini`) containing the port and database details, then run:
```bash
python python/src/main.py -c config.ini
```

### Joining an Existing Network
To start a new node and connect it to an existing network, provide the host and port of the existing node via the `-H` and `-p` arguments:
```bash
python python/src/main.py -c config_node2.ini -H <existing_node_ip> -p <existing_node_port>
```

## Experience and Skills Gained
Developing this distributed key-value store provided practical experience in:
- **Distributed Systems**: Peer-to-peer network topologies, propagation protocols, and eventual consistency.
- **Concurrency & Multithreading**: Managing race conditions in Python using `Lock()` to protect shared resources.
- **Fault Tolerance**: Implementing background recovery threads and offline queues to survive network partitions.
- **Conflict Resolution**: Building logical timestamp-based protocols to ensure data integrity across multiple writers.
- **Network Programming**: Using Flask to build APIs and internal peer-to-peer HTTP channels.
- **Database Persistence**: Integrating SQLite using robust context managers.
