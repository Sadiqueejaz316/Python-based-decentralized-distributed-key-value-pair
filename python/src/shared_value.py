import datetime
import json
import sqlite3
from typing import Dict

from statics import DISTRIBUTED_CONFIG


class SharedValue:
    def __init__(
        self,
        value: str,
        owner_id: str | None = None,
        modification_time: datetime.datetime | None = None,
    ) -> None:
        self.value = value
        self.modification_time = modification_time
        if owner_id is None:
            owner_id = DISTRIBUTED_CONFIG.self_id
        if modification_time is None:
            self.modification_time = datetime.datetime.now()
        self.owner = owner_id

    def to_json(self) -> str:
        result = {
            "value": self.value,
            "timestamp": self.modification_time.timestamp(),
            "owner_id": self.owner,
        }
        return json.dumps(result)

    def save_to_disk(self, key: str) -> None:
        if not DISTRIBUTED_CONFIG.sqlite:
            return
        delete_from_disk(key)
        with sqlite3.connect(DISTRIBUTED_CONFIG.sqlite) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO shared_values (key, value, timestamp, owner_id)
                VALUES (?, ?, ?, ?)
                """,
                (key, self.value, self.modification_time.timestamp(), self.owner),
            )


def shared_value_from_json(json_str: str) -> SharedValue:
    json_obj = json.loads(json_str)
    return SharedValue(
        value=json_obj["value"],
        owner_id=json_obj["owner_id"],
        modification_time=datetime.datetime.fromtimestamp(float(json_obj["timestamp"])),
    )


def init_db() -> None:
    if not DISTRIBUTED_CONFIG.sqlite:
        return
    with sqlite3.connect(DISTRIBUTED_CONFIG.sqlite) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS shared_values (
                key TEXT PRIMARY KEY,
                value TEXT,
                timestamp REAL,
                owner_id TEXT
            )
            """
        )

def delete_from_disk(key: str) -> None:
    if not DISTRIBUTED_CONFIG.sqlite:
        return
    with sqlite3.connect(DISTRIBUTED_CONFIG.sqlite) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE FROM shared_values
            WHERE key = ?
            """,
            (key,),
        )


def read_all_from_disk() -> Dict[str, SharedValue]:
    if not DISTRIBUTED_CONFIG.sqlite:
        return {}
    with sqlite3.connect(DISTRIBUTED_CONFIG.sqlite) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT key, value, timestamp, owner_id
            FROM shared_values
            """
        )
        results = cursor.fetchall()
        
    shared_values = {}
    for row in results:
        key, value, timestamp, owner_id = row
        shared_value = SharedValue(
            value,
            owner_id=owner_id,
            modification_time=datetime.datetime.fromtimestamp(float(timestamp)),
        )
        shared_values[key] = shared_value
    return shared_values
