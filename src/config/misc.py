import re

TIME_RE_STR = r'(?:(\d+)y)?(?:(\d+)mo)?(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?'
TIME_RE = re.compile(f'^{TIME_RE_STR}$', re.IGNORECASE)

SIZE_RE_STR = r'(\d+(?:\.(?:\d+)?)?)([KMGTP]?)I?B?'
SIZE_RE = re.compile(f'^{SIZE_RE_STR}$', re.IGNORECASE)

def parse_time(string: str | int) -> int:
    # Parses duration strings like '1d2h3m4s' in rigid order (y,mo,w,d,h,m,s).
    if isinstance(string, int):
        return string
    match = TIME_RE.match(string.lower())
    if not match or not any(match.groups()):
        raise ValueError("Invalid time format. Must be in order: y, w, d, h, m, s.")
    years, months, weeks, days, hours, minutes, seconds = (int(x) if x else 0 for x in match.groups())
    return sum((
        years * 31536000, # As 365 days
        months * 2592000, # As 30 days
        weeks * 604800,   # As 7 days
        days * 86400,     # 24 hours
        hours * 3600,     # 60 minutes
        minutes * 60,     # I mean, ...
        seconds           # Hope it is clear enough
    )) * 1_000_000_000 # Convert to nanoseconds

def parse_size(size_str: str | int) -> int:
    # Parses size strings like '10K', '20M', '1G', '500' (bytes if no suffix)
    if isinstance(size_str, int):
        if size_str < 0:
            raise ValueError("Size must be non-negative.")
        return size_str
    match = SIZE_RE.match(size_str.upper())
    if not match:
        raise ValueError("Invalid size format. Must be a number optionally followed by a SI prefix k, M, G, T.")
    size, suffix = match.groups()
    size = float(size)
    multiplier = {
        '': 1,
        'K': 1024,
        'M': 1048576,
        'G': 1073741824,
        'T': 1099511627776,
        'P': 1125899906842624
    }.get(suffix, 1)
    return int(size * multiplier)

def human_size(_size: int) -> str:
    size = float(_size)
    # Converts a size in bytes to a human-readable string with appropriate SI suffix
    if size < 0:
        raise ValueError("Size must be non-negative.")
    for unit in ['B', 'kB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0:
            size_str = f"{size:.2f}"
            size_str = size_str.rstrip('0').rstrip('.')
            return f"{size_str}{unit}"
        size /= 1024.0
    size_str = f"{size:.2f}"
    size_str = size_str.rstrip('0').rstrip('.')
    return f"{size_str}EB"

def human_time(seconds: int, max_chunks:int|None=None) -> str:
    # Converts a duration in seconds to a human-readable string
    if seconds < 0:
        raise ValueError("Time must be non-negative.")
    intervals = (
        ('y', 31536000),
        ('mo', 2592000),
        ('w', 604800),
        ('d', 86400),
        ('h', 3600),
        ('m', 60),
        ('s', 1),
    )
    result:list[str] = []
    for name, count in intervals:
        value, seconds = divmod(seconds, count)
        if max_chunks is not None and len(result)+1 >= max_chunks:
            # Last chunk, round the remaining value
            value += round(seconds / count)
            result.append(f"{value}{name}")
            break
        if value:
            result.append(f"{value}{name}")
    return ''.join(result) if result else '0s'
