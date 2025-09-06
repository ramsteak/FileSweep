from typing import Iterable, Iterator, TypeVar, Self, overload
from threading import Lock as TLock
from multiprocessing import Lock as MLock

_T = TypeVar('_T')

class MultiprocessSafeIterator(Iterator[_T]):
    def __init__(self, iterable: Iterable[_T]):
        self.iterable = iter(iterable)
        self._lock = MLock()

    def __iter__(self) -> Self:
        return self
    
    def __next__(self) -> _T:
        with self._lock:
            return next(self.iterable)

class ThreadSafeIterator(Iterator[_T]):
    def __init__(self, iterable: Iterable[_T]):
        self.iterable = iter(iterable)
        self._lock = TLock()

    def __iter__(self) -> Self:
        return self
    
    def __next__(self) -> _T:
        with self._lock:
            return next(self.iterable)

class ThreadSafeSet(set[_T]):
    @overload
    def __init__(self, /) -> None:...
    @overload
    def __init__(self, iterable: Iterable[_T], /) -> None:...
    def __init__(self, iterable: Iterable[_T] = (), /) -> None:
        super().__init__(iterable)
        self._lock = TLock()

    def add(self, item: _T) -> None:
        with self._lock:
            super().add(item)

    def remove(self, item: _T) -> None:
        with self._lock:
            super().remove(item)

    def discard(self, item: _T) -> None:
        with self._lock:
            super().discard(item)

    def __contains__(self, item: object) -> bool:
        with self._lock:
            return super().__contains__(item)

class MultiprocessSafeSet(set[_T]):
    @overload
    def __init__(self, /) -> None:...
    @overload
    def __init__(self, iterable: Iterable[_T], /) -> None:...
    def __init__(self, iterable: Iterable[_T] = (), /) -> None:
        super().__init__(iterable)
        self._lock = MLock()

    def add(self, item: _T) -> None:
        with self._lock:
            super().add(item)

    def remove(self, item: _T) -> None:
        with self._lock:
            super().remove(item)

    def discard(self, item: _T) -> None:
        with self._lock:
            super().discard(item)

    def __contains__(self, item: object) -> bool:
        with self._lock:
            return super().__contains__(item)
