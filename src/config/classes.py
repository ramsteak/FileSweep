import re

from pathlib import Path
from typing import NamedTuple, Literal

from .misc import human_size, human_time
from .policy import Policy

class FileInfo(NamedTuple):
    path: Path
    size: int
    modified: int # In seconds from Unix epoch
    accessed: int # In seconds from Unix epoch
    created: int # In seconds from Unix epoch
    inode: int
    device: int
    file_hash: str
    first_16b: str

class IncompleteFileInfo(NamedTuple):
    path: Path
    size: int
    modified: int # In seconds from Unix epoch
    accessed: int # In seconds from Unix epoch
    created: int # In seconds from Unix epoch
    inode: int
    device: int
    file_hash: None
    first_16b: None

    def complete(self, file_hash: str, first_16b: str) -> FileInfo:
        return FileInfo(
            path=self.path,
            size=self.size,
            modified=self.modified,
            accessed=self.accessed,
            created=self.created,
            inode=self.inode,
            device=self.device,
            file_hash=file_hash,
            first_16b=first_16b
        )

# Pattern matching classes for file filtering

class NamePattern(NamedTuple):
    pattern: str
    type: Literal["extension", "regex", "name"] # 'extension', 'regex', 'name'

    def match(self, file: FileInfo | IncompleteFileInfo) -> bool:
        match self.type:
            case 'extension':
                if self.pattern == '.*':
                    return True
                return file.path.suffix == self.pattern
            case 'regex':
                return re.fullmatch(self.pattern, file.path.name) is not None
            case 'name':
                if self.pattern == '*':
                    return True
                return file.path.name == self.pattern
        return False
    def __repr__(self) -> str:
        match self.type:
            case "extension":
                return f"['.{self.pattern.lstrip('.')}']"
            case "name":
                return f"['{self.pattern}']"
            case "regex":
                return f"[/{self.pattern}/]"
        return f"[?{self.pattern}]"

    
class SizePattern(NamedTuple):
    min_size: int | None
    max_size: int | None

    def match(self, file: FileInfo | IncompleteFileInfo) -> bool:
        if self.min_size is not None and file.size < self.min_size:
            return False
        if self.max_size is not None and file.size > self.max_size:
            return False
        return True
    
    def __repr__(self) -> str:
        m = human_size(self.min_size) if self.min_size is not None else ''
        M = human_size(self.max_size) if self.max_size is not None else ''
        return f"[{m}..{M}]"
    

SECONDS_IN_DAY = 86400
class DatePattern(NamedTuple):
    min: int | None
    max: int | None
    type: str  # 'modified', 'accessed', 'created'
    
    def match(self, file: FileInfo | IncompleteFileInfo) -> bool:
        from time import time_ns
        current_time = time_ns()
        modified_time = current_time - file.modified
        if self.min is not None:
            if modified_time < self.min:
                return False
        if self.max is not None:
            if modified_time > self.max:
                return False
        return True
    
    def __repr__(self) -> str:
        m = human_time(int(round(self.min/1e9))) if self.min is not None else ''
        M = human_time(int(round(self.max/1e9))) if self.max is not None else ''
        return f"[{m}..{M}]"

class Pattern(NamedTuple):
    patterns: tuple['AnyPattern', ...]
    inverted: bool
    mergemode: Literal["all", "any"]  # any for include, all for exclude

    def match(self, file: FileInfo | IncompleteFileInfo) -> bool:
        match self.inverted, self.mergemode:
            case False, 'all':
                return all(p.match(file) for p in self.patterns)
            case False, 'any':
                return any(p.match(file) for p in self.patterns)
            case True, 'all':
                return not all(p.match(file) for p in self.patterns)
            case True, 'any':
                return not any(p.match(file) for p in self.patterns)
        return False
    
    def __repr__(self) -> str:
        m = "&" if self.mergemode == "all" else "|"
        inv = "!" if self.inverted else ""
        return f"{inv}({m.join(f"{p!r}" for p in self.patterns)})"


AnyPattern = Pattern | NamePattern | DatePattern | SizePattern

class DirectoryConfig(NamedTuple):
    path: Path
    priority: int
    include_subdirs: bool | int
    policy: Policy
    rename: bool
    pattern: AnyPattern | None
    skip_subdirs: tuple[str,...]
    hidden: bool

class LoggingConfig(NamedTuple):
    level: str
    file: Path | None

class PerformanceConfig(NamedTuple):
    algorithm: str
    max_threads: int | None
    chunk_size: int | None
    max_read: int | None
    small_file_size: int | None

class GeneralConfig(NamedTuple):
    follow_symlinks: bool
    dry_run: bool
    confirm_deletion: bool
    cache_file: Path | None

class Config(NamedTuple):
    dirs: list[DirectoryConfig]
    pattern: AnyPattern
    logging: LoggingConfig | None
    performance: PerformanceConfig
    general: GeneralConfig
