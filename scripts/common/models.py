from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class Chunk:
    chunk_id: str
    doc_id: str
    text: str
    meta: Dict[str, Any]
