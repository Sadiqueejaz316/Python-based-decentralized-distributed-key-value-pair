import json
import logging
from threading import Thread, Lock
from time import sleep
from typing import List, Optional
import requests
from statics import (
    DISTRIBUTED_CONFIG,
    KNOWN_HOSTS,
    STORED_VARIABLES,
    DELAYED_UPDATE_QUEUE,
    LOST_HOSTS,
    LOST_HOSTS_LOCK,
)

from shared_value import SharedValue, shared_value_from_json

from errors import UnknownHost

MAX_RETRIES = 5
EXISTING_NETWORK_STARTUP_CONNECTION_DELAY = 10
QUEUED_DATA_LOCK = Lock()


def get_host_url_from_id(owner_id: str) -> str:
    if owner_id is None:
        return None
    if owner_id not in KNOWN_HOSTS:
        raise UnknownHost(f"Uknown host with id: {owner_id}")
    owner_host = KNOWN_HOSTS[owner_id]
    return f"http://{owner_host[0]}:{owner_host[1]}"


def try_ping(host_url: str) -> bool:
    try:
        res = requests.get(f"{host_url}/ping/")
        if res.status_code == 200 and res.text == "pong":
            return True
    except requests.RequestException:
        return False
    return False


def replace_next_host() -> None:
    if len(KNOWN_HOSTS) == 1:
        # known_hosts does include self
        DISTRIBUTED_CONFIG.next_host = None
        return
    sorted_known_hosts = sorted(KNOWN_HOSTS.keys())
    self_id_index = sorted_known_hosts.index(DISTRIBUTED_CONFIG.self_id)
    DISTRIBUTED_CONFIG.next_host = sorted_known_hosts[
        (self_id_index + 1) % len(sorted_known_hosts)
    ]
    if not try_ping(get_host_url_from_id(DISTRIBUTED_CONFIG.next_host)):
        delete_and_replace_next_host()


def delete_and_replace_next_host(
    list_of_deleted_nodes: Optional[List[str]] = None,
) -> None:
    LOST_HOSTS_LOCK.acquire()
    if len(KNOWN_HOSTS) == 1:
        # known_hosts does include self
        DISTRIBUTED_CONFIG.next_host = None
        LOST_HOSTS_LOCK.release()
        return
    if len(KNOWN_HOSTS) == 2:
        # known_hosts does include self
        LOST_HOSTS[DISTRIBUTED_CONFIG.next_host] = KNOWN_HOSTS[
            DISTRIBUTED_CONFIG.next_host
        ]
        del KNOWN_HOSTS[DISTRIBUTED_CONFIG.next_host]
        DISTRIBUTED_CONFIG.next_host = None
        LOST_HOSTS_LOCK.release()
        return
    if list_of_deleted_nodes is None:
        list_of_deleted_nodes = []
    list_of_deleted_nodes.append(DISTRIBUTED_CONFIG.next_host)
    LOST_HOSTS[DISTRIBUTED_CONFIG.next_host] = KNOWN_HOSTS[DISTRIBUTED_CONFIG.next_host]
    LOST_HOSTS_LOCK.release()
    del KNOWN_HOSTS[DISTRIBUTED_CONFIG.next_host]
    sorted_known_hosts = sorted(KNOWN_HOSTS.keys())
    self_id_index = sorted_known_hosts.index(DISTRIBUTED_CONFIG.self_id)
    DISTRIBUTED_CONFIG.next_host = sorted_known_hosts[
        (self_id_index + 1) % len(sorted_known_hosts)
    ]
    if not try_ping(get_host_url_from_id(DISTRIBUTED_CONFIG.next_host)):
        delete_and_replace_next_host(list_of_deleted_nodes)
    else:
        for deleted_host in list_of_deleted_nodes:
            propagate_delete_host_list(deleted_host)


def delayed_connect_to_existing_network(adress: str, port: int):
    sleep(EXISTING_NETWORK_STARTUP_CONNECTION_DELAY)
    connect_to_existing_network(adress, port)


def connect_to_existing_network(adress: str, port: int) -> None:
    host_url = f"http://{adress}:{port}"
    if not try_ping(host_url):
        logging.error(f"Can't connect with {host_url}")
        return
    res = None
    try:
        res = requests.get(f"{host_url}/internal/known_hosts/")
    except requests.RequestException:
        logging.error(f"Can't download known_hosts from {host_url}")
        return
    if res.status_code != 200:
        logging.error(f"Can't download known_hosts from {host_url}")
        return
    obtained_hosts = json.loads(res.text)
    logging.info("Obtained list of hosts")
    for host_id in obtained_hosts:
        if host_id in KNOWN_HOSTS.keys():
            continue
        host_details = obtained_hosts[host_id]
        KNOWN_HOSTS[host_id] = (host_details[0], host_details[1])
    logging.info("Setting up next host")
    replace_next_host()
    logging.info(
        f"Next host: {DISTRIBUTED_CONFIG.next_host} - {KNOWN_HOSTS.get(DISTRIBUTED_CONFIG.next_host)}"
    )
    if not DISTRIBUTED_CONFIG.next_host:
        logging.warning("No next host. Not connected")
        return
    next_host_url = get_host_url_from_id(DISTRIBUTED_CONFIG.next_host)

    self_host, self_port = KNOWN_HOSTS[DISTRIBUTED_CONFIG.self_id]
    data = json.dumps({"host": self_host, "port": self_port})
    response = None
    try:
        response = requests.post(
            f"{next_host_url}/internal/join/{DISTRIBUTED_CONFIG.self_id}/", data=data
        )
        if response.status_code != 200:
            logging.error("Connection failed")
            return
    except requests.RequestException:
        logging.error("Connection failed")
        return
    received_variables = json.loads(response.text)
    for variable_name in received_variables:
        variable = shared_value_from_json(received_variables[variable_name])
        if variable_name not in STORED_VARIABLES:
            STORED_VARIABLES[variable_name] = variable
            continue
        stored_variable = STORED_VARIABLES[variable_name]
        if variable.modification_time >= stored_variable.modification_time:
            STORED_VARIABLES[variable_name] = variable
            continue
        propagate_new_value(STORED_VARIABLES[variable_name], variable_name)
    for variable_name in STORED_VARIABLES:
        if variable_name not in received_variables:
            propagate_new_value(STORED_VARIABLES[variable_name], variable_name)
    for host_id in KNOWN_HOSTS:
        process_delayed_queue(host_id)


def propagate_new_value(new_value: SharedValue, value_key: str) -> None:
    host_url = get_host_url_from_id(DISTRIBUTED_CONFIG.next_host)
    if host_url is None:
        return
    update_url = f"{host_url}/internal/variable/{value_key}/update/"
    for _ in range(MAX_RETRIES):
        try:
            res = requests.post(update_url, data=new_value.to_json())
        except requests.RequestException:
            continue
        if res.status_code == 200:
            response = json.loads(res.text)
            if "status" in response and (
                response["status"] == "updated"
                or response["status"] == "already updated"
                or response["status"] == "old data"
            ):
                return
    delete_and_replace_next_host()
    propagate_new_value(new_value, value_key)


def ask_for_ownership(owner_id: str, variable_key: str) -> bool:
    host_url = ""
    try:
        host_url = get_host_url_from_id(owner_id)
    except UnknownHost as _:
        return False
    try:
        res = requests.get(
            f"{host_url}/internal/variable/{variable_key}/request_access/{DISTRIBUTED_CONFIG.self_id}/"
        )
    except requests.RequestException:
        LOST_HOSTS[owner_id] = KNOWN_HOSTS[owner_id]
        del KNOWN_HOSTS[owner_id]
        t = Thread(target=propagate_delete_host_list, args=(owner_id,))
        t.start()
        return False
    if res.status_code == 200:
        STORED_VARIABLES[variable_key].owner = DISTRIBUTED_CONFIG.self_id
        return True
    if res.status_code == 203:
        response = json.loads(res.text)
        if "owner" not in response:
            return False
        new_owner = response["owner"]
        STORED_VARIABLES[variable_key].owner = new_owner
        return ask_for_ownership(new_owner, variable_key)
    return False


def propagate_host_list(new_host_id: str, host: str, port: int):
    next_host = get_host_url_from_id(DISTRIBUTED_CONFIG.next_host)
    if not next_host:
        return
    data = json.dumps({"host": host, "port": port})
    try:
        res = requests.post(f"{next_host}/internal/new_host/{new_host_id}/", data=data)
        if res.status_code != 200:
            delete_and_replace_next_host()
            propagate_host_list(new_host_id, host, port)
    except requests.RequestException:
        delete_and_replace_next_host()
        propagate_host_list(new_host_id, host, port)


def propagate_delete_host_list(host_to_delete_id: str):
    next_host = get_host_url_from_id(DISTRIBUTED_CONFIG.next_host)
    if not next_host:
        return
    try:
        res = requests.get(f"{next_host}/internal/delete_host/{host_to_delete_id}/")
        if res.status_code != 200:
            delete_and_replace_next_host()
            propagate_delete_host_list(host_to_delete_id)
    except requests.RequestException:
        delete_and_replace_next_host()
        propagate_delete_host_list(host_to_delete_id)


def process_delayed_queue(new_host_id):
    names_to_delete = []
    QUEUED_DATA_LOCK.acquire()
    for name in DELAYED_UPDATE_QUEUE:
        new_value = DELAYED_UPDATE_QUEUE[name]
        if name in STORED_VARIABLES:
            current = STORED_VARIABLES[name]
            if current.owner != new_host_id:
                continue
            if not ask_for_ownership(current.owner, name):
                continue
        names_to_delete.append(name)
        if STORED_VARIABLES[name].modification_time > new_value.modification_time:
            # discard update since stored variable has newer timestamp
            continue
        STORED_VARIABLES[name] = new_value
        STORED_VARIABLES[name].save_to_disk(name)
        t = Thread(target=propagate_new_value, args=(new_value, name))
        t.start()
    for name in names_to_delete:
        del DELAYED_UPDATE_QUEUE[name]
    QUEUED_DATA_LOCK.release()


def propagate_force_join(new_host_ip: str, new_host_port: int) -> None:
    host_url = get_host_url_from_id(DISTRIBUTED_CONFIG.next_host)
    if host_url is None:
        return
    update_url = f"{host_url}/internal/propagate/join_to_node/"
    data = json.dumps({"host": new_host_ip, "port": new_host_port})
    for _ in range(MAX_RETRIES):
        try:
            res = requests.post(update_url, data=data)
        except requests.RequestException:
            continue
        if res.status_code == 200:
            response = json.loads(res.text)
            if "status" in response and (
                response["status"] == "updated"
                or response["status"] == "already connected"
            ):
                return
    delete_and_replace_next_host()
    propagate_force_join(new_host_ip, new_host_port)


def try_to_connect_to_lost_hosts():
    while True:
        LOST_HOSTS_LOCK.acquire()
        successfully_connected = []
        for host_id in LOST_HOSTS:
            if try_ping(f"http://{LOST_HOSTS[host_id][0]}:{LOST_HOSTS[host_id][1]}"):
                if host_id in KNOWN_HOSTS.keys():
                    # it is already connected, just remove it from lost hosts
                    successfully_connected.append(host_id)
                    continue
                t = Thread(
                    target=delayed_connect_to_existing_network,
                    args=(LOST_HOSTS[host_id][0], LOST_HOSTS[host_id][1]),
                )
                t.start()
                propagate_force_join(LOST_HOSTS[host_id][0], LOST_HOSTS[host_id][1])
                successfully_connected.append(host_id)
        for host_id in successfully_connected:
            del LOST_HOSTS[host_id]
        LOST_HOSTS_LOCK.release()
        sleep(30)
