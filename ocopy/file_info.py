from dataclasses import dataclass
from pathlib import Path


@dataclass
class FileInfo:
    source: Path
    file_hash: str
    size: int
    mtime: float
