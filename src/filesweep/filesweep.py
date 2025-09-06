import logging
import stat

from send2trash import send2trash

from dataclasses import dataclass
from enum import Enum
from functools import total_ordering
from os import utime, getenv
from pathlib import Path
from queue import Queue, Empty
from time import perf_counter
from threading import Thread
from typing import Iterable

## IMPORTANT:
# As of now, hardlinks are not supported. The database treats paths as unique identifiers.
# If a file is hardlinked in multiple locations, it will result in file collisions. 

from filesweep import __version__
from filesweep.config import load_config, Config, policy_priority, Policy, human_size
from filesweep.config.classes import LoggingConfig, FileInfo, DirectoryConfig, IncompleteFileInfo
from filesweep.config.load import read_file_info
from filesweep.hasher import hash_file, read_16b
from filesweep.statdb import StatDB
from filesweep.threadsafe import ThreadSafeIterator, ThreadSafeSet

@total_ordering
class Action(Enum):
    KEEP = 10
    RETIME = 7
    LINK = 5
    TRASH = 2
    DELETE = 1
    NOACTION = 0
    UNDEFINED = -1

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Action):
            return NotImplemented
        return self.value < other.value

@dataclass(slots=True)
class Decision:
    dircfg: DirectoryConfig  # None if no matching config
    file_index: int  # Index in the database, None if new file
    file_info: FileInfo
    action: Action  # keep, delete, link, retime, None
    target: Path | None = None  # Target path for linking, None for keep or delete
    time: int | None = None # New modified time for renaming, None otherwise

def init_logger(logger: LoggingConfig):

    # Set level based on config
    level = getattr(logging, logger.level.upper(), logging.INFO)
    # Set the format to be:
    # [year-month-day hour:minute:second] [LEVEL] <logger name>: message]
    # without milliseconds 
    log_format = "[%(asctime)s] [%(levelname)s] <%(name)s>: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    handlers:list[logging.Handler] = []
    handlers.append(logging.StreamHandler())
    if logger.file is not None:
        handlers.append(logging.FileHandler(logger.file, encoding="utf-8") )

    logging.basicConfig(encoding="utf-8", level=level, handlers=handlers, format=log_format, datefmt=date_format)

def find_config_file() -> Path:
    # Look for config file in the following locations, in order:
    # 1. Environment variable FILESWEEP_CONFIG
    # 2. Current directory ./filesweep.yml
    # 3. User home directory ~/.filesweep.yml
    # 4. /etc/filesweep.yml (Linux only)
    # If none found, raise FileNotFoundError
    
    env_var = "FILESWEEP_CONFIG"
    env_var_value = getenv(env_var)
    possible_locations: list[Path] = []

    if env_var_value is not None:
        possible_locations.append(Path(env_var_value))
    
    possible_locations += [
        Path.home() / ".filesweep" / "config.yaml",
        Path.home() / ".filesweep" / "config.yml",

        Path.home() / ".config" / "filesweep" / "config.yaml",
        Path.home() / ".config" / "filesweep" / "config.yml",
        Path.home() / ".config" / "config.yaml",
        Path.home() / ".config" / "config.yml",

        Path.home() / ".filesweep.yaml",
        Path.home() / ".filesweep.yml",

        Path.cwd() / "filesweep.yaml",
        Path.cwd() / "filesweep.yml",
        Path.cwd() / "config.yaml",
        Path.cwd() / "config.yml",
    ]

    if Path("/etc").exists():
        possible_locations += [
            Path("/etc/filesweep/filesweep.yaml"),
            Path("/etc/filesweep/filesweep.yml"),
            Path("/etc/filesweep/config.yaml"),
            Path("/etc/filesweep/config.yml"),
            Path("/etc/filesweep.yaml"),
            Path("/etc/filesweep.yml")
        ]

    for loc in possible_locations:
        if loc.is_file():
            return loc
            
    errmsg = (
        f"No configuration file found. Add a valid config in one of the following locations:\n"
        f" - ~/.config/filesweep/  (recommended)\n"
        f" - {Path.home()}  (user directory)\n"
        f" - {Path.cwd()}  (current directory)\n"
        f" - /etc/filesweep/\n"
        f" - /etc/\n"
        f"Or set the environment variable {env_var} to point to a valid config file.\n"
        f"Valid config files are named filesweep.yaml (recommended) or config.yaml"
    )
    print(errmsg)
    exit(1)

def init(config_file: str|Path) -> tuple[Config, StatDB]:
    # Load configuration settings
    config = load_config(config_file)

    # Load existing cache or initialize a new one
    db = StatDB(config.general.cache_file)

    if config.logging is not None:
        init_logger(config.logging)
    else:
        init_logger(LoggingConfig("ERROR", None))

    return config, db

def _is_hidden(path: Path) -> bool:
    if path.stat().st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN:
        return True
    if path.name.startswith('.'):
        return True
    return False

def _iterate_dir(directory: Path, current_depth: int, subdir_depth: int, dir_config: DirectoryConfig) -> Iterable[Path]:
    for entry in directory.iterdir():
        if entry.is_file():
            if not dir_config.hidden and _is_hidden(entry):
                continue
            yield entry
        else:
            # TODO: check symlinks based on config
            if entry.is_dir() and current_depth < subdir_depth:
                if entry.name in dir_config.skip_subdirs:
                    continue
                if not dir_config.hidden and _is_hidden(entry):
                    continue
                yield from _iterate_dir(entry, current_depth + 1, subdir_depth, dir_config)

def iterate_files(config: Config) -> Iterable[IncompleteFileInfo]:
    # Get an iterator over all files in the configured directories.
    # Respect include_subdirs and follow_symlinks settings.
    # Hash is not calculated here, f16b and hash are set to None.
    # Does not filter by pattern, size or date.
    for d in config.dirs:
        # Make subdirs into an integer: 0 -> False, True -> high value
        if d.include_subdirs is True:
            subdirs = 4096 #4294967295
        elif d.include_subdirs is False:
            subdirs = 0
        else:
            subdirs = d.include_subdirs

        yield from (read_file_info(path) for path in _iterate_dir(d.path, 0, subdirs, d) if path.is_file())

def _get_directory_config_for_path(file_info: FileInfo | IncompleteFileInfo, dir_cfgs: list[DirectoryConfig]) -> DirectoryConfig | None:
    # Given a path and a dict of directory configs, return the config that matches the path.
    # If any config has a pattern, the file must match it.
    # If multiple configs match, return the one with the longest path (most specific).
    # Among configs with the same path length, return the one with the pattern that matches.
    # If all have patterns, check the policy. Keep the one with the highest policy.
    # If no config matches, return None.

    valid_configs: dict[DirectoryConfig, int] = {
        dcfg: file_info.path.parents.index(dcfg.path)
        for dcfg in dir_cfgs
        if (
            file_info.path.is_relative_to(dcfg.path) and (
                # If the config has a pattern, the file must match it.
                dcfg.pattern is None or dcfg.pattern.match(file_info)
            )
        )
    }
    
    if not valid_configs:
        return None
    
    configs = sorted(valid_configs, key=lambda e: valid_configs[e])

    # Check if any config has a pattern
    if any(dcfg.pattern is not None for dcfg in valid_configs):
        # Remove configs without pattern
        configs = [dcfg for dcfg in configs if dcfg.pattern is not None]
    
    # Check the priority of the remaining configs, get only ones with the highest priority.
    highest_priority = max(dcfg.priority for dcfg in configs)
    configs = [dcfg for dcfg in configs if dcfg.priority == highest_priority]

    # If multiple configs remain, return the one with the most restrictive policy (highest)
    if len(configs) == 0:
        raise RuntimeError("No valid directory configurations found, this should not happen.")
    if len(configs) == 1:
        return configs[0]

    return max(configs, key=lambda dcfg: policy_priority(dcfg.policy))

def _add_new_files_th(iter: ThreadSafeIterator[IncompleteFileInfo], config: Config, db: StatDB, checked_files: ThreadSafeSet[Path]):
    log = logging.getLogger("filesweep")
    for file_info_inc in iter:
        # Check if the file matches the global pattern
        if not config.pattern.match(file_info_inc):
            continue
        if file_info_inc.path in checked_files:
            # Happens if multiple directories overlap
            continue

        _dircfg = _get_directory_config_for_path(file_info_inc, config.dirs)
        
        if _dircfg is None:
            continue

        checked_files.add(file_info_inc.path)

        # Check if the file is already in the database
        # First check by path, then by inode.

        db_entry_bypath = db.get_item(path = file_info_inc.path, return_index=True)
        if db_entry_bypath is not None:
            _, db_entry_bypath = db_entry_bypath
        db_entry_bydvin = db.get_item(device_inode = (file_info_inc.device, file_info_inc.inode), return_index=True)
        if db_entry_bydvin is not None:
            db_entry_bydvin_idx, db_entry_bydvin = db_entry_bydvin
        else:
            db_entry_bydvin_idx = -1

        # Possible scenarios:
        # 1. File is in the database by path and inode, with same path: update its info if needed
        # 2. File is in the database by inode only: it was moved/renamed, update its path and info if needed
        # 3. File is in the database by path only: it was replaced by another file, treat as new file
        # 4. File is not in the database: new file, add it
        
        # action, item, old, old_idx = None, None, None, None
        old_idx = -1 # Unbound check
        try:
            match db_entry_bypath, db_entry_bydvin:
                case _, None:
                    # New file, add it to the database
                    f16b = read_16b(file_info_inc.path)
                    hash = hash_file(file_info_inc.path, config.performance.algorithm, config.performance.chunk_size, config.performance.max_read)
                    action = "add"
                    item = file_info_inc.complete(first_16b=f16b, file_hash=hash)

                case None, FileInfo() as file_info_db:
                    # File was probably moved/renamed. Check stats, if necessary then
                    # check file content. If small file, hash, else first check f16b.
                    # If hash matches, update the path in the database.
                    # If not, treat as new file.
                    
                    f16b = read_16b(file_info_inc.path)
                    # Check stats against already known file
                    if (file_info_inc.size != file_info_db.size) or (file_info_inc.modified != file_info_db.modified):
                        # Size or modified time differs, treat as new file
                        hash = hash_file(file_info_inc.path, config.performance.algorithm, config.performance.chunk_size, config.performance.max_read)
                        item = file_info_inc.complete(first_16b=f16b, file_hash=hash)
                        action = "add"
                    else:

                        # Check f16b / hash. If config.performance.small_file_size is None, always hash.
                        if config.performance.small_file_size is None or file_info_inc.size <= config.performance.small_file_size:
                            # Small file, hash it directly
                            hash = hash_file(file_info_inc.path, config.performance.algorithm, config.performance.chunk_size, config.performance.max_read)
                            if hash == file_info_db.file_hash:
                                # Same file, update path
                                action = "update"
                                item = file_info_db._replace(path=file_info_inc.path)
                                old_idx = db_entry_bydvin_idx
                            else:
                                # Different file, add as new
                                action = "add"
                                item = file_info_inc.complete(first_16b=f16b, file_hash=hash)
                        else:
                            # Large file, check first 16 bytes
                            if f16b == file_info_db.first_16b:
                                # Same file, update path
                                action = "update"
                                item = file_info_db._replace(path=file_info_inc.path)
                                old_idx = db_entry_bydvin_idx
                            else:
                                # Different file, hash and add as new
                                hash = hash_file(file_info_inc.path, config.performance.algorithm, config.performance.chunk_size, config.performance.max_read)
                                action = "add"
                                item = file_info_inc.complete(first_16b=f16b, file_hash=hash)
                    
                case FileInfo() as file_info_db_1, FileInfo() as file_info_db_2:
                    # The file is already in the database, matching by both path and inode.
                    # Check if the two entries are the same. If not, log a warning and skip the file.
                    # If they are the same, do nothing.
                    if file_info_db_1.path != file_info_db_2.path:
                        log.warning(f"File {file_info_inc.path} has conflicting database entries. Consider deleting cached data. Skipping...")
                    log.debug(f"Processed file: {file_info_db_1.path} (mtime: {file_info_db_1.modified}, size: {file_info_db_1.size}, hash: {file_info_db_1.file_hash})")
                    continue

                case _:
                    raise RuntimeError("Unhandled database entry case, this should not happen.")
            
            match action:
                case "add":
                    db.add_item(item)
                    log.info(f"Added file: {item.path} (size: {item.size}, modified: {item.modified}, hash: {item.file_hash})")
                case "update":
                    if old_idx < 0:
                        raise RuntimeError("Old index is -1, this should not happen.")
                    db.update_item(item, old_idx)
                    log.info(f"Updated file: {item.path} (size: {item.size}, modified: {item.modified}, hash: {item.file_hash})")
            
            log.debug(f"Processed file: {item.path} (mtime: {item.modified}, size: {item.size}, hash: {item.file_hash})")
        
        except (PermissionError, FileNotFoundError) as e:
            log.error(f"Error accessing file {file_info_inc.path}: {e}")


def _add_new_files(config: Config, db: StatDB) -> set[Path]:
    log = logging.getLogger("filesweep")

    _checked_files: set[Path] = ThreadSafeSet()
    _all_files = iterate_files(config)
    all_files = ThreadSafeIterator(_all_files)

    nthreads = config.performance.max_threads or 1
    log.debug(f"Starting new file check with {nthreads} threads...")

    threads = [Thread(target=_add_new_files_th, args=(all_files, config, db, _checked_files), daemon=True)
               for _ in range(config.performance.max_threads or 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    return _checked_files

def _check_stale_files(checked_files: set[Path], db: StatDB):
    log = logging.getLogger("filesweep")
    _db_paths = set(db.path_index.keys())
    stale_paths = _db_paths - checked_files

    for stale_path in stale_paths:
        db.pop_item(path = stale_path)
        log.info(f"Removed stale file from database: {stale_path}")

def update_db(config: Config, db: StatDB):
    log = logging.getLogger("filesweep")
    log.info("Updating database...")

    _checked_files = _add_new_files(config, db)
    _check_stale_files(_checked_files, db)
    
    log.info(f"Database update complete. {len(db)} entries in database.")

def check_db(config: Config, db: StatDB, decision_queue: Queue[Decision]):
    log = logging.getLogger("filesweep")
    # Read the entire database, grouped by hash. For each group, decide what to do.
    for _, idxs in db.hash_index.groups():
        pass
        # if len(idxs) == 1: continue
        
        # We have duplicates with the given hash. Decide what to do with them.
        # Get all configs for each file. Will be stored in the decision struct.
        hash_decisions: dict[int, Decision] = {}
        for idx in idxs:
            file_info = db.get_item(index = idx)
            if file_info is None:
                log.error(f"Error retrieving file info for index {idx}, skipping...")
                continue

            # Get the directory config for this file as a dict, with config as key and
            # path depth as value.
            _dircfg = _get_directory_config_for_path(file_info, config.dirs)
            if _dircfg is None:
                # No matching config, keep the file and log warning
                log.warning(f"File {file_info.path} has no matching directory configuration, keeping by default.")
                hash_decisions[idx] = Decision(None, idx, file_info, action=Action.NOACTION) # No config, keep but do not check # type: ignore
            else:
                hash_decisions[idx] = Decision(_dircfg, idx, file_info, action=Action.UNDEFINED)
        
        # Now we have the decision dict[index, Decision] for each file, we can decide what to do.

        # All files without a matching config are kept (config = None, already set above).

        # For the rest of the files:
        # Check the policy for each directory config:
        #  - keep: keep the file
        #  - delete: delete the file if there is another file with policy keep or link
        #  - link: do nothing for the moment TODO
        # If there are multiple files with policy keep, keep all of them.
        # If there are multiple files with policy link, link all of them to the one with the highest priority.
        # If all files have policy delete, keep the one with the highest priority and delete the rest.
        
        # First pass: determine policies. Get highest policy and decision.
        # Highest decision represents the winner file.
        # Hardlinks will point to this, delete will keep this, etc.
        _highest_policy = max(f.dircfg.policy for f in hash_decisions.values())
        _highest_decision = max(
            (d for d in hash_decisions.values() if d.dircfg.policy == _highest_policy),
            # Among those with the highest policy, get the one with the highest priority and oldest modified time.
            key=lambda d: (d.dircfg.priority, -d.file_info.modified)
        )
        # If there is at least one `keep`, keep all `keep` and delete all `delete` and link all `link` to the highest priority `keep`.
        # If there is no `keep` but at least one `link`, link all `link` to the highest priority `link` and delete all `delete`.
        # If there is only `delete`, keep the highest priority `delete` and delete the rest.
        for idx, decision in hash_decisions.items():
            match decision, _highest_decision:
                case a,b if a.dircfg.policy > b.dircfg.policy:
                    # The highest policy is always highest (should always be true)
                    raise RuntimeError("This should not happen, highest policy is not highest in the list.")
                
                case a,b if a == b and a.dircfg.policy == Policy.DISCARD:
                    # This is the highest priority file. Discard policy means send to trash even if no duplicates.
                    decision.action = Action.TRASH
                case a,b if a == b and a.dircfg.policy == Policy.ERASE:
                    # This is the highest priority file. Erase policy means delete even if no duplicates.
                    decision.action = Action.DELETE

                # Check if we are in a rename folder with TRASH or DELETE policy
                case a,b if a == b and a.dircfg.rename and a.dircfg.policy in (Policy.TRASH, Policy.DELETE):
                    # This is the highest priority file. We need to retime it to the newest file time.
                    decision.action = Action.RETIME
                    # If no other file, retime to its own modified time
                    if _highest_decision.time is None:
                        _highest_decision.time = a.file_info.modified
                    else:
                        _highest_decision.time = max(_highest_decision.time, a.file_info.modified)
                    decision.time

                case a,b if a.dircfg.path == b.dircfg.path and a.dircfg.rename and a.dircfg.policy in (Policy.TRASH, Policy.DELETE):
                    # We are in the same folder as the winner file, but this is not the highest priority file.
                    # This file will not be kept, and the winner file will be retimed to the newest modified time.
                    if b.action in (Action.UNDEFINED, Action.RETIME):
                        b.action = Action.RETIME
                        if _highest_decision.time is None:
                            _highest_decision.time = a.file_info.modified
                        else:
                            _highest_decision.time = max(_highest_decision.time, a.file_info.modified)
                    else:
                        raise RuntimeError("This should not happen, retime action already set to something else.")
                    
                    if a.dircfg.policy == Policy.TRASH:
                        decision.action = Action.TRASH
                        decision.target = b.file_info.path
                    elif a.dircfg.policy == Policy.DELETE:
                        decision.action = Action.DELETE
                        decision.target = b.file_info.path
                    else:
                        raise RuntimeError("This should not happen, invalid policy for retime.")
                    
                case a,b if a == b:
                    # In all other cases, take no action for the highest priority file.
                    decision.action = Action.NOACTION

                case a, _ if a.dircfg.policy == Policy.KEEP:
                    # Always keep
                    decision.action = Action.KEEP
                
                case a, _ if a.dircfg.policy == Policy.PROMPT:
                    # Not implemented, treat as keep
                    log.warning(f"Policy prompt not yet implemented, treating as keep for file {decision.file_info.path}...")
                    decision.action = Action.KEEP
                case a, _ if a.dircfg.policy == Policy.HARDLINK:
                    # Not implemented, treat as keep
                    log.warning(f"Policy hardlink not yet implemented, treating as keep for file {decision.file_info.path}...")
                    decision.action = Action.KEEP
                
                case a, b if a.dircfg.policy == Policy.TRASH and b.dircfg.policy >= Policy.TRASH:
                    # Trash if there is a higher policy, else take no action
                    decision.action = Action.TRASH
                    decision.target = b.file_info.path
                
                case a, b if a.dircfg.policy == Policy.DELETE and b.dircfg.policy >= Policy.DELETE:
                    # Delete if there is a higher policy, else take no action
                    decision.action = Action.DELETE
                    decision.target = b.file_info.path

                case _:
                    decision.action = Action.NOACTION

        for final_decision in hash_decisions.values():
            # Check retime action. If time is the same as current file time, convert to noaction.
            if final_decision.action == Action.RETIME and final_decision.time == final_decision.file_info.modified:
                final_decision.action = Action.NOACTION
                final_decision.time = None
            
            log.debug(f"Decision for file {final_decision.file_info.path}: {final_decision.action.name} (policy: {final_decision.dircfg.policy.name if final_decision.dircfg else 'None'}, target: {final_decision.target})")
            decision_queue.put(final_decision)

def act_decisions(decision_queue: Queue[Decision], db: StatDB, dry_run: bool) -> int:
    saved_space = 0
    log = logging.getLogger("filesweep")

    try:
        while True:
            decision = decision_queue.get(block=False)

            match decision.action:
                case Action.UNDEFINED:
                    log.error(f"Undefined action for file {decision.file_info.path}, skipping...")
                    continue
                case Action.NOACTION:
                    log.debug(f"Keeping file {decision.file_info.path} (no action).")
                case Action.KEEP:
                    log.info(f"Keeping file {decision.file_info.path}.")
                case Action.RETIME:
                    if dry_run:
                        log.info(f"Dry run: would update modified time of file {decision.file_info.path} to {decision.time}.")
                    else:
                        if decision.time is None:
                            log.error(f"Retime action for file {decision.file_info.path} has no time set, skipping...")
                            continue
                        utime(decision.file_info.path, ns=(decision.file_info.accessed, decision.time))
                        log.info(f"Updated modified time of file {decision.file_info.path} to {decision.time}.")
                case Action.LINK:
                    log.warning(f"Hardlinking not yet implemented, keeping file {decision.file_info.path}")
                case Action.TRASH:
                    if dry_run:
                        log.info(f"Dry run: would send to trash file {decision.file_info.path}" + (f", duplicate of {decision.target}" if decision.target else ""))
                        saved_space += decision.file_info.size
                    else:
                        try:
                            send2trash(str(decision.file_info.path))
                            db.pop_item(index = decision.file_index)
                            saved_space += decision.file_info.size
                            log.info(f"Sent to trash file {decision.file_info.path}"  + (f", duplicate of {decision.target}" if decision.target else "") + ". Freed {human_size(decision.file_info.size)}.")
                        except Exception as e:
                            log.error(f"Error sending file {decision.file_info.path} to trash: {e}")
                case Action.DELETE:
                    if dry_run:
                        log.info(f"Dry run: would delete file {decision.file_info.path}.")
                        saved_space += decision.file_info.size
                    else:
                        try:
                            decision.file_info.path.unlink()
                            db.pop_item(index = decision.file_index)
                            saved_space += decision.file_info.size
                            log.info(f"Deleted file {decision.file_info.path}, freed {human_size(decision.file_info.size)}.")
                        except Exception as e:
                            log.error(f"Error deleting file {decision.file_info.path}: {e}")

            decision_queue.task_done()
    except Empty:
        return saved_space
    except Exception as e:
        log.error(f"Error processing decisions: {e}")
    
    return saved_space

def main(config: Config, db: StatDB):
    # Initial logging information
    log = logging.getLogger("filesweep")
    log.info(f"Starting FileSweep {__version__} ")
    log.info("Loaded configuration")
    
    db.load()
    log.info(f"Loaded database with {len(db)} entries from {config.general.cache_file}")
    log.debug("Configured global pattern:")
    log.debug(f"  {config.pattern!r}")
    log.debug("Configured directories:")
    for d in sorted(config.dirs, key=lambda d: d.priority):
        log.debug(f" - {d.priority:3d} {d.path / ("*" if d.include_subdirs else "")}")
        if d.pattern is not None:
            log.debug(f"       Pattern: {d.pattern!r}")

    # Update the database with all files currently present in the configured directories.
    update_db(config, db)
    
    # Check the entire database.
    # The decision queue stores decisions to be made about all files.
    decision_queue = Queue[Decision]()
    check_db(config, db, decision_queue)

    # Act on the decisions in the queue.
    saved_space = act_decisions(decision_queue, db, config.general.dry_run)

    if config.general.dry_run:
        log.info("Dry run complete. No files were deleted or modified.")
        log.info(f"Total space that would be saved: {human_size(saved_space)}")
    else:
        log.info(f"Total space saved: {human_size(saved_space)}")

def run() -> None:
    # Entry point for the application
    
    config_file = find_config_file()
    config, db = init(config_file)
    # Catch SIGINT and SIGTERM to save the database before exiting
    log = logging.getLogger("exit")
    try:
        t0 = perf_counter()
        main(config, db)
        t1 = perf_counter()
        log.info(f"Program completed in {t1 - t0:.2f} seconds.")
    except (KeyboardInterrupt, SystemExit):
        log.info("Program interrupted.")
    finally:
        log.info("Saving database...")
        db.save()

if __name__ == '__main__':
    run()