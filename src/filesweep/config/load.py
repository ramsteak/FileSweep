import re
import yaml

from pathlib import Path
from typing import Any, TypeVar, Sequence

from filesweep.config.classes import Config, AnyPattern, Pattern, NamePattern, SizePattern, DatePattern, DirectoryConfig, LoggingConfig, PerformanceConfig, GeneralConfig, IncompleteFileInfo
from filesweep.config.misc import parse_time, parse_size, SIZE_RE_STR, TIME_RE_STR
from filesweep.config.policy import Policy

_K = TypeVar('_K')
_V = TypeVar('_V')
def items(d: dict[_K, _V] | Sequence[tuple[_K, _V]] | tuple[_K, _V], /) -> Sequence[tuple[_K, _V]]:
    if isinstance(d, dict):
        return list(d.items())
    if isinstance(d, tuple) and len(d) == 2:
        return [d] # type: ignore
    return d

def _load_config(config_path: str|Path) -> dict[str, Any]:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def _load_pattern(pattern_cfg: dict[str, Any]) -> AnyPattern:
    # Recursively load patterns
    # Get include/exclude
    patterns: list[AnyPattern] = []
    inverted = False
    mergemode = 'all' # Default for include

    # If 'pattern' in pattern_cfg, only load the pattern configuration from the string.
    if 'pattern' in pattern_cfg:
        pattern_str = pattern_cfg['pattern']
        ptn = _parse_pattern_fromstr(pattern_str)
        if ptn is None:
            raise ValueError(f"Invalid pattern string: {pattern_str}")
        return ptn

    for action in items(pattern_cfg):
        match action:
            case ['include', action_cfg]:
                ptns = [_load_pattern(ptn_cfg) for ptn_cfg in action_cfg.items()]
                patterns.append(Pattern(tuple(ptns), inverted=False, mergemode='all'))
            case ['exclude', action_cfg]:
                ptns = [_load_pattern(ptn_cfg) for ptn_cfg in action_cfg.items()]
                patterns.append(Pattern(tuple(ptns), inverted=True, mergemode='any'))
            case ['name', [*names]]:
                mergemode = 'any'

                for name in names:
                    if name.startswith('.'):
                        patterns.append(NamePattern(name, 'extension'))
                    elif name.startswith('/') and name.endswith('/'):
                        patterns.append(NamePattern(name[1:-1], 'regex'))
                    else:
                        patterns.append(NamePattern(name, 'name'))
            case ['size', size_cfg]:
                min_size_ = size_cfg.get('min', None)
                max_size_ = size_cfg.get('max', None)
                min_size = parse_size(min_size_) if min_size_ is not None else None
                max_size = parse_size(max_size_) if max_size_ is not None else None
                return SizePattern(min_size, max_size)
            case ['modified' | 'accessed' | 'created' as mode, date_cfg]:
                min_ = date_cfg.get('min', None)
                max_ = date_cfg.get('max', None)
                min = parse_time(min_) if min_ is not None else None
                max = parse_time(max_) if max_ is not None else None
                return DatePattern(min, max, mode)
            case [unknown, mode]:
                raise ValueError(f"Unknown pattern action: {unknown} with config {mode}")
            
    return Pattern(tuple(patterns), inverted, mergemode)

def _read_path(path: str) -> Path:
    if path.startswith('~'):
        return Path(path).expanduser()
    return Path(path)

def load_config(config_path: str|Path) -> Config:
    config_dict = _load_config(config_path)

    dirs_cfg:list[dict[str,str|Any]] = config_dict.get('directories', [])
    dirs:list[DirectoryConfig] = []
    for d in dirs_cfg:
        _pattern = d.get('pattern', None)
        if _pattern is not None:
            _pattern = _parse_pattern_fromstr(_pattern)
        skip_subdirs_cfg = d.get('skip_subdirs', [])

        dir = DirectoryConfig(
                Path(d['path']),
                int(d.get('priority', 0)),
                bool(d.get('subdirs', True)),
                Policy(d.get('policy', Policy.PROMPT)),
                bool(d.get('rename', False)),
                _pattern,
                tuple(skip_subdirs_cfg),
                bool(d.get('hidden', False)),
            )
        dirs.append(dir)

    pattern_cfg = config_dict.get('match')
    if pattern_cfg is None:
        pattern = Pattern((), False, 'all') # All with no patterns returns True
    else:
        pattern = _load_pattern(pattern_cfg)
    
    logging_cfg = config_dict.get('logging', {})
    logging_pth = logging_cfg.get('file', None)
    
    logging = LoggingConfig(
        logging_cfg.get('level', 'INFO'),
        _read_path(logging_pth) if logging_pth is not None else None,
    )

    performance_cfg = config_dict.get('performance', {})
    max_read = performance_cfg.get('max_read', None)
    chunk_size = performance_cfg.get('chunk_size', None)
    small_file_size = performance_cfg.get('small_file_size', None)

    performance = PerformanceConfig(
        performance_cfg.get('algorithm', 'md5'),
        performance_cfg.get('max_threads', None),
        parse_size(chunk_size) if chunk_size is not None else None,
        parse_size(max_read) if max_read is not None else None,
        parse_size(small_file_size) if small_file_size is not None else None,
    )

    general_cfg = config_dict.get('general', {})
    cache_path = general_cfg.get('cache_file', None)
    general = GeneralConfig(
        general_cfg.get('follow_symlinks', False),
        general_cfg.get('dry_run', False),
        general_cfg.get('confirm_deletion', True),
        _read_path(cache_path) if cache_path is not None else None,
    )

    return Config(dirs, pattern, logging, performance, general)

def read_file_info(path: Path) -> IncompleteFileInfo:
    stat = path.lstat()
    return IncompleteFileInfo(
        path=path,
        size=stat.st_size,
        modified=int(stat.st_mtime_ns),
        accessed=int(stat.st_atime_ns),
        created=int(stat.st_birthtime_ns),
        inode=stat.st_ino,
        device=stat.st_dev,
        file_hash=None,
        first_16b=None
    )

def _parse_pattern_fromstr(pattern_str: str) -> AnyPattern | None:
    """
    Simple parser for patterns from string. Supports nested patterns from their
    string representation.
     - [..] will be skipped, as it is ambiguous without context.
     - [..8d] will be treated as date pattern, from 0 to 8 days ago
     - [1KB..] will be treated as size pattern, from 1KB to unlimited
     - ['.*'] will be treated as an extension name pattern, any file with an extension
     - ['name'] will be treated as a name pattern, any file with that exact name (glob-supported)
     - [/^.*$/] will be treated as regex pattern
    Patterns can be combined with & (and) and | (or), and negated with ! (not)
    Parentheses can be used to group patterns. Within parentheses, only one of
    & or | can be used.
    
    e.g. ((['.*'])&[10.00B..10.00GB]&[0s..])
    Will match files with any extension, size between 10 bytes and 10 gigabytes,
    and age at least 0 seconds.
    """
    pattern_str = pattern_str.strip()
    if pattern_str.startswith('[') and pattern_str.endswith(']'):
        pattern_str = pattern_str[1:-1].strip()
        # The pattern is either a NamePattern, SizePattern or DatePattern

        # NamePattern
        ## Extension -> starts with '.
        ## Regex -> starts and ends with /
        ## Name -> otherwise

        if pattern_str == '..': # Ambiguous without context
            return None
        elif (m:=re.match(r"^'(\..*)'$", pattern_str)): # Extension
            return NamePattern(m.group(1), 'extension')
        elif (m:=re.match(r"^/(.*)/$", pattern_str)): # Regex
            return NamePattern(m.group(1), 'regex')
        elif (m:=re.match(r"^'(.*)'$", pattern_str)): # Name
            return NamePattern(m.group(1), 'name')
        
        # SizePattern
        # Number with unit, range defined by ..: 10KB.., ..10MB, 10KB..10MB
        # If minimum is bigger than maximum, return None
        elif (m:=re.match(f"^(?P<l>{SIZE_RE_STR})?..(?P<h>{SIZE_RE_STR})?$", pattern_str.upper())):
            min_size_ = parse_size(m.group('l')) if m.group('l') != '' else None
            max_size_ = parse_size(m.group('h')) if m.group('h') != '' else None
            if min_size_ is not None and max_size_ is not None and min_size_ > max_size_:
                return None
            return SizePattern(min_size_, max_size_)
        
        # DatePattern
        # Number with unit, range defined by ..: 0s.., 6m..10d.
        # If minimum is bigger than maximum, return None 
        elif (m:=re.match(f"^(?P<l>{TIME_RE_STR})?..(?P<h>{TIME_RE_STR})?$", pattern_str)):
            min_time_ = parse_time(m.group('l')) if m.group('l') != '' else None
            max_time_ = parse_time(m.group('h')) if m.group('h') != '' else None
            if min_time_ is not None and max_time_ is not None and min_time_ > max_time_:
                return None
            return DatePattern(min_time_, max_time_, 'modified') # Default to modified time

    else:
        # The pattern is a nested pattern. Parse it recursively.
        # If negation, it is the first character.
        if pattern_str.startswith('!'):
            inverted = True
            pattern_str = pattern_str[1:].strip()
        else:
            inverted = False
        # The pattern will have outside parentheses, and contain patterns separated by & or |.
        if not (pattern_str.startswith('(') and pattern_str.endswith(')')):
            raise ValueError(f"Invalid pattern string: {pattern_str}. Nested patterns must be enclosed in parentheses.")
        # Remove outside parentheses
        pattern_str = pattern_str[1:-1].strip()

        subpatterns_str:list[str] = []
        mergemode = None # Default case. If only one pattern is present, it doesn't matter.
        depth = 0
        startidx = 0
        for idx, chr in enumerate(pattern_str):
            if chr == '(':
                depth += 1
            elif chr == ')':
                depth -= 1
            
            elif depth == 0 and chr in '&|':
                # Split here
                subpatterns_str.append(pattern_str[startidx:idx].strip())
                startidx = idx + 1
                if mergemode is None or mergemode == chr:
                    mergemode = chr
                else:
                    raise ValueError(f"Invalid pattern string: {pattern_str}. Cannot mix '&' and '|' at the same level.")
        else:
            # Last pattern leftover
            subpatterns_str.append(pattern_str[startidx:].strip())
        
        subpatterns:list[AnyPattern] = []
        for subpattern_str in subpatterns_str:
            subpattern = _parse_pattern_fromstr(subpattern_str)
            if subpattern is not None:
                subpatterns.append(subpattern)
        if mergemode is None or mergemode == '&':
            mergemode = 'all'
        else:
            mergemode = 'any'
        
        return Pattern(tuple(subpatterns), inverted, mergemode)
