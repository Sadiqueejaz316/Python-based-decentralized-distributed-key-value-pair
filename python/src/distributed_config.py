from typing import Optional, Tuple


class DistributedConfiguration:
    self_id = ""
    next_host: Optional[Tuple[str, int]] = None
    sqlite: str = None
