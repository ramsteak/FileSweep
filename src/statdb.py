from __future__ import annotations

import gzip
import json

from pathlib import Path
from threading import Lock
from typing import Generic, TypeVar, Collection, Iterable, overload, Literal, TypedDict

from .config.classes import FileInfo

def _ser_fileinfo(f: FileInfo) -> dict[str, str|int|None]:
    return {
        'fp': str(f.path),
        'sz': f.size,
        'mt': f.modified,
        'at': f.accessed,
        'ct': f.created,
        'in': f.inode,
        'dv': f.device,
        'fh': f.file_hash,
        '16': f.first_16b,
    }

def _de_fileinfo(d: dict[str, str|int|None]) -> FileInfo:
    return FileInfo(
        Path(str(d['fp'])),
        int(d['sz'] or -1),
        int(d['mt'] or -1),
        int(d['at'] or -1),
        int(d['ct'] or -1),
        int(d['in'] or -1),
        int(d['dv'] or -1),
        str(d['fh']),
        str(d['16']),
    )

class SaveDataRepresentation(TypedDict):
    files: list[dict[str, str|int|None]]
    collisions: list[tuple[str, str]]
class SaveData(TypedDict):
    files: list[FileInfo]
    collisions: list[tuple[Path, Path]]

def _load_cache(path: Path) -> SaveData:
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        data = json.load(f)
        _files_d = data.get('files', [])
        _collisions_d = data.get('collisions', [])
        files: list[FileInfo] = [_de_fileinfo(v) for v in _files_d]
        collisions: list[tuple[Path, Path]] = [(Path(p1), Path(p2)) for p1, p2 in _collisions_d]
        return SaveData(files=files, collisions=collisions)

def _save_cache(path: Path, cache_data: Iterable[FileInfo], accepted_collisions: Iterable[tuple[Path, Path]]) -> None:
    cache = [_ser_fileinfo(v) for v in cache_data]
    collisions = [ (str(p1), str(p2)) for p1, p2 in accepted_collisions ]

    data = SaveDataRepresentation(files=cache, collisions=collisions)
    # Compress cache with gzip to save space
    gzip_file = gzip.open(path, 'wt', encoding='utf-8')
    with gzip_file as f:
        json.dump(data, f)

_K = TypeVar('_K')
_V = TypeVar('_V')
class Bag(Collection[_K], Generic[_K, _V]):
    '''A bag is a dictionary that maps keys to lists of values.'''

    def __init__(self) -> None:
        self._data: dict[_K, list[_V]] = {}
        
    @classmethod
    def from_iter(cls, iterable: Iterable[tuple[_K, _V]]) -> Bag[_K, _V]:
        bag = cls()
        for k, v in iterable:
            bag.add(k, v)
        return bag
    
    def __getitem__(self, key: _K) -> list[_V]:
        return self._data[key]
    
    def add(self, key: _K, value: _V) -> None:
        if key not in self:
            self._data[key] = []
        self[key].append(value)

    def __delitem__(self, key: _K) -> None:
        del self._data[key]
    
    def remove(self, key: _K, value: _V) -> None:
        if key in self:
            try:
                self[key].remove(value)
                if not self[key]:
                    del self[key]
            except ValueError:
                pass
    
    def __iter__(self):
        return iter(self._data)
    
    def __len__(self) -> int:
        return len(self._data)
    
    def __contains__(self, key: object) -> bool:
        return self._data.__contains__(key)
    
    def groups(self) -> Iterable[tuple[_K, list[_V]]]:
        return self._data.items()
    
    def items(self) -> Iterable[tuple[_K, _V]]:
        yield from ((k, v) for k, vs in self._data.items() for v in vs)

    def __repr__(self) -> str:
        return f"Bag({self._data})"
    
    def __str__(self) -> str:
        return str(self._data)
    
    def copy(self) -> Bag[_K, _V]:
        new_bag = Bag[_K, _V]()
        for k, vs in self._data.items():
            new_bag._data[k] = vs.copy()
        return new_bag
    
    def clear(self) -> None:
        self._data.clear()

class ItemExistsError(Exception):...
class ItemNotFoundError(Exception):...
class InvalidItemError(Exception):...

class StatDB():
    # Database that stores the file stats and hashes, and indexes them as a database
    # to avoid recomputing them on boot and to allow for quick lookups.
    # The database is stored on disk as a compressed JSON file, and loaded into
    # memory as multiple dictionaries for quick access.
    # On disk, the database is stored as:
    #       list[FileInfo]
    # From this data, we can build the following dicts:
    #       index -> FileInfo           dict
    #       path -> index               dict
    #       hash string -> list[index]  Bag
    #       first bytes -> list[index]  Bag
    def __init__(self, cache_path: Path | None):
        self.cache_path = cache_path
        self._index = 0
        self._lock = Lock()
        self._dirty = None
    
    def _next_index(self) -> int:
        self._index += 1
        return self._index
    
    def load(self) -> None:
        with self._lock:
            if self._dirty is not None:
                raise RuntimeError("Database is already loaded.")
            self._dirty = False
            # Initialize database and indexes
            # Main index, from database index to FileInfo
            self.file_info: dict[int, FileInfo] = {}            # index -> FileInfo
            # Unique indexes, from file / inode to index.
            # No two files can have the same path or inode/device.
            self.path_index: dict[Path, int] = {}               # Path -> index
            self.dvin_index: dict[tuple[int, int], int] = {}    # (inode, device) -> index
            # Non-unique indexes, from hash / first 16 bytes to list of indexes.
            # File contents may be duplicates, so these map to lists of indexes.
            self.hash_index: Bag[str, int] = Bag()              # hash -> list[index]
            self.f16b_index: Bag[str, int] = Bag()              # first 16 bytes -> list[index]

            if self.cache_path is None:
                return
            
            # Load cache data: Path -> FileInfo
            try:
                data = _load_cache(self.cache_path)
                cache, accepted = data["files"], data["collisions"]
            except FileNotFoundError:
                cache:list[FileInfo] = []
                accepted:list[tuple[Path,Path]] = []

            # Add items to database
            for finfo in cache:
                self._add_item(finfo)

            self.accepted_collisions = { (p1, p2) for p1, p2 in accepted }
            self.accepted_collisions.update( (p2, p1) for p1, p2 in accepted ) # Make symmetric
    
    def save(self) -> None:
        with self._lock:
            if self._dirty is None:
                raise RuntimeError("Database is not loaded.")
            if not self._dirty:
                return
            if self.cache_path is None:
                raise ValueError("Cache path is None, cannot save cache.")
            # Desymmetrize accepted collisions
            acceptedc = { (p1, p2) for p1, p2 in self.accepted_collisions if p1 < p2 }
            _save_cache(self.cache_path, self.file_info.values(), acceptedc)

    def _add_item(self, finfo: FileInfo) -> int:
        # Add item to database and indexes. Does not perform any check.
        idx = self._next_index()

        self.file_info[idx] = finfo
        self.path_index[finfo.path] = idx
        self.hash_index[finfo.file_hash] = idx # type: ignore
        self.f16b_index[finfo.first_16b] = idx # type: ignore
        self.dvin_index[finfo.device, finfo.inode] = idx

        self.hash_index.add(finfo.file_hash, idx)
        self.f16b_index.add(finfo.first_16b, idx)
    
        return idx

    def add_item(self, finfo: FileInfo) -> int:
        # Ensure item is valid and hashes are computed.
        # if finfo.file_hash is None or finfo.first_16b is None:
        #     raise InvalidItemError("FileInfo must have file_hash and first_16b computed.")

        with self._lock:
            # Check if path already exists
            if finfo.path in self.path_index:
                raise ItemExistsError(f"Item with path {finfo.path} already exists.")
            
            self._dirty = True
            return self._add_item(finfo)

    
    @overload
    def pop_item(self, *, index: int) -> FileInfo: ...
    @overload
    def pop_item(self, *, path: Path) -> FileInfo: ...
    @overload
    def pop_item(self, *, device_inode: tuple[int,int]) -> FileInfo: ...
    def pop_item(self, *, index: int | ellipsis = ..., path: Path | ellipsis = ..., device_inode: tuple[int,int] | ellipsis = ...) -> FileInfo: # type: ignore
        with self._lock:
            if index is not ...:
                if index not in self.file_info:
                    raise ItemNotFoundError(f"Item with index {index} not found.")
                finfo = self.file_info[index]
            elif path is not ...:
                if path not in self.path_index:
                    raise ItemNotFoundError(f"Item with path {path} not found.")
                index = self.path_index[path]
                finfo = self.file_info[index]
            elif device_inode is not ...:
                index = self.dvin_index[device_inode]
                finfo = self.file_info[index]
            else:
                raise ValueError("Either index or path must be provided.")
            
            # Remove item from all indexes
            self._dirty = True
            self.file_info.pop(index)
            self.path_index.pop(finfo.path)
            self.hash_index.remove(finfo.file_hash, index)
            self.f16b_index.remove(finfo.first_16b, index)
            self.dvin_index.pop((finfo.device, finfo.inode))
            return finfo
    
    @overload
    def _get_item(self, *, index: int) -> tuple[int, FileInfo]: ...
    @overload
    def _get_item(self, *, path: Path) -> tuple[int, FileInfo]: ...
    @overload
    def _get_item(self, *, device_inode: tuple[int,int]) -> tuple[int, FileInfo]: ...
    def _get_item(self, *, index: int | ellipsis = ..., path: Path | ellipsis = ..., device_inode: tuple[int,int] | ellipsis = ...) -> tuple[int, FileInfo]: # type: ignore
        # Assumes either index or path is valid and exists. Raises KeyError if not.
        if index is not ...:
            return index, self.file_info[index]
        elif path is not ...:
            idx = self.path_index[path]
            return idx, self.file_info[idx]
        elif device_inode is not ...:
            idx = self.dvin_index[device_inode]
            return idx, self.file_info[idx]
        else:
            raise ValueError("Either index, path or device/inode must be provided.")
    
    @overload
    def get_item(self, *, index: int, return_index: Literal[False] = False) -> FileInfo | None: ...
    @overload
    def get_item(self, *, path: Path, return_index: Literal[False] = False) -> FileInfo | None: ...
    @overload
    def get_item(self, *, device_inode: tuple[int,int], return_index: Literal[False] = False) -> FileInfo | None: ...
    @overload
    def get_item(self, *, index: int, return_index: Literal[True]) -> tuple[int, FileInfo] | None: ...
    @overload
    def get_item(self, *, path: Path, return_index: Literal[True]) -> tuple[int, FileInfo] | None: ...
    @overload
    def get_item(self, *, device_inode: tuple[int,int], return_index: Literal[True]) -> tuple[int, FileInfo] | None: ...

    def get_item(self, *, index: int|ellipsis = ..., path: Path|ellipsis = ..., device_inode: tuple[int,int]|ellipsis = ..., return_index:bool=False) -> FileInfo | tuple[int, FileInfo] | None: # type: ignore
        # Wraps _get_item and returns None if not found.
        with self._lock:
            try:
                if return_index:
                    if index is not ...:
                        return self._get_item(index=index)
                    if path is not ...:
                        return self._get_item(path=path)
                    if device_inode is not ...:
                        return self._get_item(device_inode=device_inode)
                    raise ValueError("One of index, path, or device_inode must be provided.")
                else:
                    if index is not ...:
                        return self._get_item(index=index)[1]
                    if path is not ...:
                        return self._get_item(path=path)[1]
                    if device_inode is not ...:
                        return self._get_item(device_inode=device_inode)[1]
                    raise ValueError("One of index, path, or device_inode must be provided.")
            except KeyError:
                return None
    
    def get_items(self, *, index: int|ellipsis = ..., path: Path|ellipsis = ..., file_hash: str|ellipsis = ..., first_16b: str|ellipsis = ...) -> list[FileInfo]: # type: ignore
        with self._lock:
            if index is not ...:
                try:
                    _, item = self._get_item(index=index)
                    return [item]
                except KeyError:
                    return []
            elif path is not ...:
                try:
                    _, item = self._get_item(path=path)
                    return [item]
                except KeyError:
                    return []
            elif file_hash is not ...:
                if file_hash in self.hash_index:
                    return [self.file_info[idx] for idx in self.hash_index[file_hash]]
                else:
                    return []
            elif first_16b is not ...:
                if first_16b in self.f16b_index:
                    return [self.file_info[idx] for idx in self.f16b_index[first_16b]]
                else:
                    return []
            else:
                raise ValueError("One of index, path, file_hash, or first_16b must be provided.")

    def update_item(self, info: FileInfo, index:int|None=None) -> int:
        with self._lock:
            if info.path not in self.path_index:
                raise ItemNotFoundError(f"Item with path {info.path} not found.")
            # if info.file_hash is None or info.first_16b is None:
            #     raise InvalidItemError("FileInfo must have file_hash and first_16b computed.")
            
            self._dirty = True

            if index is None:
                idx = self.path_index[info.path]
                old_info = self.file_info[idx]
            else:
                if index not in self.file_info:
                    raise ItemNotFoundError(f"Item with index {index} not found.")
                idx = index
                old_info = self.file_info[idx]
                if old_info.path != info.path:
                    raise InvalidItemError("Cannot change path of existing item.")

            # Remove old indexes
            self.hash_index.remove(old_info.file_hash, idx)
            self.f16b_index.remove(old_info.first_16b, idx)

            # Update info
            self.file_info[idx] = info

            # Add new indexes
            self.hash_index.add(info.file_hash, idx)
            self.f16b_index.add(info.first_16b, idx)

            return idx

    def __len__(self) -> int:
        with self._lock:
            return len(self.file_info)