import base64
import datetime
import json
import logging
import os
import signal
import sys
from time import sleep
import socket
import configparser
from threading import Thread, Lock
from typing import Optional, Tuple
from flask import Flask, request, Response
from statics import (
    DISTRIBUTED_CONFIG,
    KNOWN_HOSTS,
    DELAYED_UPDATE_QUEUE,
    STORED_VARIABLES,
    AWAITING_CONNECT_HOST,
    LOST_HOSTS_LOCK,
)

from shared_value import (
    SharedValue,
    shared_value_from_json,
    read_all_from_disk,
    delete_from_disk,
    init_db,
)
from helper_functions import *

app = Flask(__name__)
UPDATE_VARIABLE_LOCK = Lock()


@app.route("/variable/<name>/")
def get_variable(name: str):
    return json.dumps(STORED_VARIABLES[name].value) if name in STORED_VARIABLES else ""


@app.route("/variable/<name>/update/", methods=["POST"])
def update_variable(name: str):
    data = request.get_data()
    new_value = SharedValue(data.decode())
    UPDATE_VARIABLE_LOCK.acquire()
    if name in STORED_VARIABLES:
        current = STORED_VARIABLES[name]
        if current.owner != DISTRIBUTED_CONFIG.self_id:
            if not current.owner in KNOWN_HOSTS:
                DELAYED_UPDATE_QUEUE[name] = new_value
                UPDATE_VARIABLE_LOCK.release()
                return Response(json.dumps({"status": "queued"}), status=202)
            if not ask_for_ownership(current.owner, name):
                DELAYED_UPDATE_QUEUE[name] = new_value
                UPDATE_VARIABLE_LOCK.release()
                return Response(json.dumps({"status": "queued"}), status=202)
        if current.modification_time > new_value.modification_time:
            new_value.modification_time = (
                new_value.modification_time + datetime.timedelta(milliseconds=1)
            )
    STORED_VARIABLES[name] = new_value
    STORED_VARIABLES[name].save_to_disk(name)
    UPDATE_VARIABLE_LOCK.release()
    propagate_thread = Thread(target=propagate_new_value, args=(new_value, name))
    propagate_thread.start()
    return json.dumps({"status": "updated"})


@app.route("/internal/variable/<name>/")
def internal_get_variable(name: str):
    return (
        STORED_VARIABLES[name].to_json()
        if name in STORED_VARIABLES
        else Response("", status=404)
    )


@app.route("/internal/variable/<name>/update/", methods=["POST"])
def internal_update_variable(name: str):
    data = request.get_data()

    new_data = shared_value_from_json(data.decode())
    UPDATE_VARIABLE_LOCK.acquire()
    if (
        name in STORED_VARIABLES
        and STORED_VARIABLES[name].value == new_data.value
        and STORED_VARIABLES[name].modification_time == new_data.modification_time
    ):
        UPDATE_VARIABLE_LOCK.release()
        return json.dumps({"status": "already updated"})
    if (
        name in STORED_VARIABLES
        and STORED_VARIABLES[name].modification_time >= new_data.modification_time
    ):
        UPDATE_VARIABLE_LOCK.release()
        return json.dumps({"status": "old data"})
    STORED_VARIABLES[name] = new_data
    UPDATE_VARIABLE_LOCK.release()
    propagate_thread = Thread(
        target=propagate_new_value, args=(STORED_VARIABLES[name], name)
    )
    propagate_thread.start()
    return json.dumps({"status": "updated"})


@app.route("/internal/variable/<name>/request_access/<owner_id>/")
def internal_request_access_variable(name: str, owner_id: str):
    if name not in STORED_VARIABLES:
        return json.dumps({"owner": owner_id})
    variable = STORED_VARIABLES[name]
    if variable.owner == DISTRIBUTED_CONFIG.self_id:
        variable.owner = owner_id
        delete_from_disk(name)
        return json.dumps({"owner": owner_id})
    return Response(json.dumps({"owner": variable.owner}), status=203)


@app.route("/internal/known_hosts/")
def internal_get_known_hosts():
    return json.dumps(KNOWN_HOSTS)


@app.route("/internal/new_host/<connecting_id>/", methods=["POST"])
def internal_connect(connecting_id: str):
    if connecting_id == DISTRIBUTED_CONFIG.self_id:
        return json.dumps({"status": "success"})
    data = request.get_data()
    new_host = json.loads(data.decode())
    # {"host": "192.168.1.1", "port": 5000}
    new_host_ip = new_host["host"]
    new_host_port = new_host["port"]
    KNOWN_HOSTS[connecting_id] = (new_host_ip, new_host_port)
    if not try_ping(get_host_url_from_id(connecting_id)):
        return Response(json.dumps({"status": "fail"}), status=500)
    replace_next_host()
    t = Thread(
        target=propagate_host_list, args=(connecting_id, new_host_ip, new_host_port)
    )
    t.start()
    t2 = Thread(target=process_delayed_queue, args=(connecting_id,))
    t2.start()
    return json.dumps({"status": "success"})


@app.route("/internal/join/<connecting_id>/", methods=["POST"])
def internal_join(connecting_id: str):
    if connecting_id == DISTRIBUTED_CONFIG.self_id:
        return Response(json.dumps({"status": "failed"}), status=400)
    data = request.get_data()
    new_host = json.loads(data.decode())
    # {"host": "192.168.1.1", "port": 5000}
    new_host_ip = new_host["host"]
    new_host_port = new_host["port"]
    if not try_ping(f"http://{new_host_ip}:{new_host_port}"):
        return Response(json.dumps({"status": "failed"}), status=400)
    KNOWN_HOSTS[connecting_id] = (new_host_ip, new_host_port)
    replace_next_host()
    t = Thread(
        target=propagate_host_list, args=(connecting_id, new_host_ip, new_host_port)
    )
    t.start()
    t2 = Thread(target=process_delayed_queue, args=(connecting_id,))
    t2.start()
    all_variables = {}
    for name in STORED_VARIABLES:
        all_variables[name] = STORED_VARIABLES[name].to_json()
    return json.dumps(all_variables)


@app.route("/internal/delete_host/<delete_id>/")
def internal_delete_host(delete_id: str):
    if delete_id == DISTRIBUTED_CONFIG.self_id:
        return Response(json.dumps({"status": "failed"}), status=400)
    if delete_id in KNOWN_HOSTS:
        LOST_HOSTS_LOCK.acquire()
        LOST_HOSTS[delete_id] = KNOWN_HOSTS[delete_id]
        del KNOWN_HOSTS[delete_id]
        LOST_HOSTS_LOCK.release()
    else:
        return json.dumps({"status": "success"})
    replace_next_host()
    t = Thread(target=propagate_delete_host_list, args=(delete_id,))
    t.start()
    return json.dumps({"status": "success"})


@app.route("/internal/propagate/join_to_node/", methods=["POST"])
def internal_force_join():
    data = request.get_data()
    new_host = json.loads(data.decode())
    # {"host": "192.168.1.1", "port": 5000}
    new_host_ip = new_host["host"]
    new_host_port = new_host["port"]
    if not try_ping(f"http://{new_host_ip}:{new_host_port}"):
        return Response(json.dumps({"status": "failed"}), status=400)
    if (new_host_ip, new_host_port) in AWAITING_CONNECT_HOST:
        return json.dumps({"status": "already connected"})
    for host_id in KNOWN_HOSTS:
        host_ip, host_port = KNOWN_HOSTS[host_id]
        if host_ip == new_host_ip and host_port == new_host_port:
            return json.dumps({"status": "already connected"})

    AWAITING_CONNECT_HOST.add((new_host_ip, new_host_port))
    propagate_thread = Thread(
        target=propagate_force_join, args=(new_host_ip, new_host_port)
    )
    propagate_thread.start()
    t = Thread(
        target=delayed_connect_to_existing_network, args=(new_host_ip, new_host_port)
    )
    t.start()
    return json.dumps({"status": "success"})


@app.route("/ping/")
def ping_pong():
    return "pong"


def get_lan_ip():
    # Python hack to get local ip
    # requires internet connection, used once during start
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))  # 8.8.8.8 is google dns, high availability, called once
    ip_addr = s.getsockname()[0]
    s.close()
    return ip_addr


def run_flask(host: str, port: int):
    app.run(host=host, port=port, debug=False)


def signal_handler(a, b):
    logging.info("Exiting program...")
    os.kill(os.getpid(), signal.SIGINT)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Distributed Key-Value Store Node")
    parser.add_argument("-c", "--config", required=True, help="Path to config.ini file")
    parser.add_argument("-H", "--host", dest="existing_host", help="Host address of existing network node")
    parser.add_argument("-p", "--port", dest="existing_port", type=int, help="Port of existing network node")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    config = configparser.ConfigParser()
    config.read(args.config)

    host = get_lan_ip()
    port = int(config["Node"]["port"])
    
    DISTRIBUTED_CONFIG.self_id = base64.encodebytes(f"{host}:{port}".encode()).decode()
    DISTRIBUTED_CONFIG.self_id = DISTRIBUTED_CONFIG.self_id[:-1]  # remove newline
    DISTRIBUTED_CONFIG.sqlite = config["Node"]["database"]
    
    logging.info(f'Self id: "{DISTRIBUTED_CONFIG.self_id}"')
    
    KNOWN_HOSTS[DISTRIBUTED_CONFIG.self_id] = (host, port)
    
    init_db()
    saved_variables = read_all_from_disk()
    STORED_VARIABLES.update(saved_variables)
    
    if args.existing_host and args.existing_port:
        existing_network_details = (args.existing_host, args.existing_port)
        existing_network_connect_thread = Thread(
            target=delayed_connect_to_existing_network, args=existing_network_details
        )
        existing_network_connect_thread.start()
        
    reconnect_thread = Thread(target=try_to_connect_to_lost_hosts)
    reconnect_thread.start()
    
    flask_thread = Thread(target=run_flask, args=(host, port))
    signal.signal(signal.SIGINT, signal_handler)
    flask_thread.start()
    flask_thread.join()
