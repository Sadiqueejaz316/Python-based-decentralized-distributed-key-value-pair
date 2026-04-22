import threading
from typing import Dict, Set, Tuple, TYPE_CHECKING
from distributed_config import DistributedConfiguration

if TYPE_CHECKING:
    # Hack to avoid cyclic import
    from shared_value import SharedValue


DISTRIBUTED_CONFIG = DistributedConfiguration()
KNOWN_HOSTS: Dict[str, Tuple[str, int]] = {}
LOST_HOSTS: Dict[str, Tuple[str, int]] = {}

AWAITING_CONNECT_HOST: Set[Tuple[str, int]] = set()

DELAYED_UPDATE_QUEUE: Dict[str, "SharedValue"] = {}
STORED_VARIABLES: Dict[str, "SharedValue"] = {}

LOST_HOSTS_LOCK = threading.Lock()
