from __future__ import annotations

"""
############################################
portalocker - Cross-platform locking library
############################################

.. image:: https://github.com/WoLpH/portalocker/actions/workflows/python-package.yml/badge.svg?branch=master
    :alt: Linux Test Status
    :target: https://github.com/WoLpH/portalocker/actions/

.. image:: https://ci.appveyor.com/api/projects/status/mgqry98hgpy4prhh?svg=true
    :alt: Windows Tests Status
    :target: https://ci.appveyor.com/project/WoLpH/portalocker

.. image:: https://coveralls.io/repos/WoLpH/portalocker/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://coveralls.io/r/WoLpH/portalocker?branch=master

Overview
--------

Portalocker is a library to provide an easy API to file locking.

An important detail to note is that on Linux and Unix systems the locks are
advisory by default. By specifying the `-o mand` option to the mount command it
is possible to enable mandatory file locking on Linux. This is generally not
recommended however. For more information about the subject:

 - https://en.wikipedia.org/wiki/File_locking
 - http://stackoverflow.com/questions/39292051/portalocker-does-not-seem-to-lock
 - https://stackoverflow.com/questions/12062466/mandatory-file-lock-on-linux

The module is currently maintained by Rick van Hattem <Wolph@wol.ph>.
The project resides at https://github.com/WoLpH/portalocker . Bugs and feature
requests can be submitted there. Patches are also very welcome.

Security contact information
------------------------------------------------------------------------------

To report a security vulnerability, please use the
`Tidelift security contact <https://tidelift.com/security>`_.
Tidelift will coordinate the fix and disclosure.

Redis Locks
-----------

This library now features a lock based on Redis which allows for locks across
multiple threads, processes and even distributed locks across multiple
computers.

It is an extremely reliable Redis lock that is based on pubsub.

As opposed to most Redis locking systems based on key/value pairs,
this locking method is based on the pubsub system. The big advantage is
that if the connection gets killed due to network issues, crashing
processes or otherwise, it will still immediately unlock instead of
waiting for a lock timeout.

First make sure you have everything installed correctly:

::

    pip install "portalocker[redis]"

Usage is really easy:

::

    import portalocker

    lock = portalocker.RedisLock('some_lock_channel_name')

    with lock:
        print('do something here')

The API is essentially identical to the other ``Lock`` classes so in addition
to the ``with`` statement you can also use ``lock.acquire(...)``.

Python 2
--------

Python 2 was supported in versions before Portalocker 2.0. If you are still
using
Python 2,
you can run this to install:

::

    pip install "portalocker<2"

Tips
----

On some networked filesystems it might be needed to force a `os.fsync()` before
closing the file so it's actually written before another client reads the file.
Effectively this comes down to:

::

   with portalocker.Lock('some_file', 'rb+', timeout=60) as fh:
       # do what you need to do
       ...

       # flush and sync to filesystem
       fh.flush()
       os.fsync(fh.fileno())

Links
-----

* Documentation
    - http://portalocker.readthedocs.org/en/latest/
* Source
    - https://github.com/WoLpH/portalocker
* Bug reports
    - https://github.com/WoLpH/portalocker/issues
* Package homepage
    - https://pypi.python.org/pypi/portalocker
* My blog
    - http://w.wol.ph/

Examples
--------

To make sure your cache generation scripts don't race, use the `Lock` class:

>>> import portalocker
>>> with portalocker.Lock('somefile', timeout=1) as fh:
...     print('writing some stuff to my cache...', file=fh)

To customize the opening and locking a manual approach is also possible:

>>> import portalocker
>>> file = open('somefile', 'r+')
>>> portalocker.lock(file, portalocker.LockFlags.EXCLUSIVE)
>>> file.seek(12)
>>> file.write('foo')
>>> file.close()

Explicitly unlocking is not needed in most cases but omitting it has been known
to cause issues:
https://github.com/AzureAD/microsoft-authentication-extensions-for-python/issues/42#issuecomment-601108266

If needed, it can be done through:

>>> portalocker.unlock(file)

Do note that your data might still be in a buffer so it is possible that your
data is not available until you `flush()` or `close()`.

To create a cross platform bounded semaphore across multiple processes you can
use the `BoundedSemaphore` class which functions somewhat similar to
`threading.BoundedSemaphore`:

>>> import portalocker
>>> n = 2
>>> timeout = 0.1

>>> semaphore_a = portalocker.BoundedSemaphore(n, timeout=timeout)
>>> semaphore_b = portalocker.BoundedSemaphore(n, timeout=timeout)
>>> semaphore_c = portalocker.BoundedSemaphore(n, timeout=timeout)

>>> semaphore_a.acquire()
<portalocker.utils.Lock object at ...>
>>> semaphore_b.acquire()
<portalocker.utils.Lock object at ...>
>>> semaphore_c.acquire()
Traceback (most recent call last):
  ...
portalocker.exceptions.AlreadyLocked


More examples can be found in the
`tests <http://portalocker.readthedocs.io/en/latest/_modules/tests/tests.html>`_.


Versioning
----------

This library follows `Semantic Versioning <http://semver.org/>`_.


Changelog
---------

Every release has a ``git tag`` with a commit message for the tag
explaining what was added and/or changed. The list of tags/releases
including the commit messages can be found here:
https://github.com/WoLpH/portalocker/releases

License
-------

See the `LICENSE <https://github.com/WoLpH/portalocker/blob/develop/LICENSE>`_ file.


"""

"""
Copyright 2022 Rick van Hattem

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""

__package_name__ = 'portalocker'
__author__ = 'Rick van Hattem'
__email__ = 'wolph@wol.ph'
__version__ = '3.2.0'
__description__ = """Wraps the portalocker recipe for easy usage"""
__url__ = 'https://github.com/WoLpH/portalocker'
"""
Locking constants

Lock types:

- `EXCLUSIVE` exclusive lock
- `SHARED` shared lock

Lock flags:

- `NON_BLOCKING` non-blocking

Manually unlock, only needed internally

- `UNBLOCK` unlock
"""

import enum
import os

# The actual tests will execute the code anyhow so the following code can
# safely be ignored from the coverage tests
if os.name == 'nt':  # pragma: no cover
    import msvcrt

    #: exclusive lock
    LOCK_EX = 0x1
    #: shared lock
    LOCK_SH = 0x2
    #: non-blocking
    LOCK_NB = 0x4
    #: unlock
    LOCK_UN = msvcrt.LK_UNLCK  # type: ignore[attr-defined]

elif os.name == 'posix':  # pragma: no cover
    import fcntl

    #: exclusive lock
    LOCK_EX = fcntl.LOCK_EX  # type: ignore[attr-defined]
    #: shared lock
    LOCK_SH = fcntl.LOCK_SH  # type: ignore[attr-defined]
    #: non-blocking
    LOCK_NB = fcntl.LOCK_NB  # type: ignore[attr-defined]
    #: unlock
    LOCK_UN = fcntl.LOCK_UN  # type: ignore[attr-defined]

else:  # pragma: no cover
    raise RuntimeError('PortaLocker only defined for nt and posix platforms')


class LockFlags(enum.IntFlag):
    #: exclusive lock
    EXCLUSIVE = LOCK_EX
    #: shared lock
    SHARED = LOCK_SH
    #: non-blocking
    NON_BLOCKING = LOCK_NB
    #: unlock
    UNBLOCK = LOCK_UN


# noqa: A005
import io
import pathlib
import typing
from typing import Union

# spellchecker: off
# fmt: off
Mode = typing.Literal[
    # Text modes
    # Read text
    'r', 'rt', 'tr',
    # Write text
    'w', 'wt', 'tw',
    # Append text
    'a', 'at', 'ta',
    # Exclusive creation text
    'x', 'xt', 'tx',
    # Read and write text
    'r+', '+r', 'rt+', 'r+t', '+rt', 'tr+', 't+r', '+tr',
    # Write and read text
    'w+', '+w', 'wt+', 'w+t', '+wt', 'tw+', 't+w', '+tw',
    # Append and read text
    'a+', '+a', 'at+', 'a+t', '+at', 'ta+', 't+a', '+ta',
    # Exclusive creation and read text
    'x+', '+x', 'xt+', 'x+t', '+xt', 'tx+', 't+x', '+tx',
    # Universal newline support
    'U', 'rU', 'Ur', 'rtU', 'rUt', 'Urt', 'trU', 'tUr', 'Utr',

    # Binary modes
    # Read binary
    'rb', 'br',
    # Write binary
    'wb', 'bw',
    # Append binary
    'ab', 'ba',
    # Exclusive creation binary
    'xb', 'bx',
    # Read and write binary
    'rb+', 'r+b', '+rb', 'br+', 'b+r', '+br',
    # Write and read binary
    'wb+', 'w+b', '+wb', 'bw+', 'b+w', '+bw',
    # Append and read binary
    'ab+', 'a+b', '+ab', 'ba+', 'b+a', '+ba',
    # Exclusive creation and read binary
    'xb+', 'x+b', '+xb', 'bx+', 'b+x', '+bx',
    # Universal newline support in binary mode
    'rbU', 'rUb', 'Urb', 'brU', 'bUr', 'Ubr',
]
# spellchecker: on
Filename = Union[str, pathlib.Path]
IO = Union[  # type: ignore[name-defined]
    typing.IO[str],
    typing.IO[bytes],
]


class FileOpenKwargs(typing.TypedDict):
    buffering: int | None
    encoding: str | None
    errors: str | None
    newline: str | None
    closefd: bool | None
    opener: typing.Callable[[str, int], int] | None


# Protocol for objects with a fileno() method.
# Used for type-hinting fcntl.flock.
class HasFileno(typing.Protocol):
    def fileno(self) -> int: ...

# Type alias for file arguments used in lock/unlock functions
FileArgument = Union[typing.IO[typing.Any], io.TextIOWrapper, int, HasFileno]


class BaseLockException(Exception):  # noqa: N818
    # Error codes:
    LOCK_FAILED: typing.Final = 1

    strerror: typing.Optional[str] = None  # ensure attribute always exists

    def __init__(
        self,
        *args: typing.Any,
        fh: typing.Union[IO, None, int, HasFileno] = None,
        **kwargs: typing.Any,
    ) -> None:
        self.fh = fh
        self.strerror = (
            str(args[1])
            if len(args) > 1 and isinstance(args[1], str)
            else None
        )
        Exception.__init__(self, *args)


class LockException(BaseLockException):
    pass


class AlreadyLocked(LockException):
    pass


class FileToLarge(LockException):
    pass
# pyright: reportUnknownMemberType=false, reportAttributeAccessIssue=false
"""Module portalocker.

This module provides cross-platform file locking functionality.
The Windows implementation now supports two variants:

  1. A default method using the Win32 API (win32file.LockFileEx/UnlockFileEx).
  2. An alternative that uses msvcrt.locking for exclusive locks (shared
     locks still use the Win32 API).

This version uses classes to encapsulate locking logic, while maintaining
the original external API, including the LOCKER constant for specific
backwards compatibility (POSIX) and Windows behavior.
"""

import io
import os
import typing
from typing import (
    Any,
    Callable,
    Optional,
    Union,
    cast,
)

# Alias for readability


# Define a protocol for callable lockers
class LockCallable(typing.Protocol):
    def __call__(
        self, file_obj: FileArgument, flags: LockFlags
    ) -> None: ...


class UnlockCallable(typing.Protocol):
    def __call__(self, file_obj: FileArgument) -> None: ...


class BaseLocker:
    """Base class for locker implementations."""

    def lock(self, file_obj: FileArgument, flags: LockFlags) -> None:
        """Lock the file."""
        raise NotImplementedError

    def unlock(self, file_obj: FileArgument) -> None:
        """Unlock the file."""
        raise NotImplementedError


# Define refined LockerType with more specific types
LockerType = Union[
    # POSIX-style fcntl.flock callable
    Callable[[Union[int, HasFileno], int], Any],
    # Tuple of lock and unlock functions
    tuple[LockCallable, UnlockCallable],
    # BaseLocker instance
    BaseLocker,
    # BaseLocker class
    type[BaseLocker],
]

LOCKER: LockerType

if os.name == 'nt':  # pragma: not-posix
    # Windows-specific helper functions
    def _prepare_windows_file(
        file_obj: FileArgument,
    ) -> tuple[int, Optional[typing.IO[Any]], Optional[int]]:
        """Prepare file for Windows: get fd, optionally seek and save pos."""
        if isinstance(file_obj, int):
            # Plain file descriptor
            return file_obj, None, None

        # Full IO objects (have tell/seek) -> preserve and restore position
        if isinstance(file_obj, io.IOBase):
            fd: int = file_obj.fileno()
            original_pos = file_obj.tell()
            if original_pos != 0:
                file_obj.seek(0)
            return fd, typing.cast(typing.IO[Any], file_obj), original_pos
            # cast satisfies mypy: IOBase -> IO[Any]

        # Fallback: an object that only implements fileno() (HasFileno)
        fd = typing.cast(HasFileno, file_obj).fileno()  # type: ignore[redundant-cast]
        return fd, None, None

    def _restore_windows_file_pos(
        file_io_obj: Optional[typing.IO[Any]],
        original_pos: Optional[int],
    ) -> None:
        """Restore file position if it was an IO object and pos was saved."""
        if file_io_obj and original_pos is not None and original_pos != 0:
            file_io_obj.seek(original_pos)

    class Win32Locker(BaseLocker):
        """Locker using Win32 API (LockFileEx/UnlockFileEx)."""

        _overlapped: Any  # pywintypes.OVERLAPPED
        _lock_bytes_low: int = -0x10000

        def __init__(self) -> None:
            try:
                import pywintypes
            except ImportError as e:
                raise ImportError(
                    'pywintypes is required for Win32Locker but not '
                    'found. Please install pywin32.'
                ) from e
            self._overlapped = pywintypes.OVERLAPPED()

        def _get_os_handle(self, fd: int) -> int:
            try:
                import msvcrt
            except ImportError as e:
                raise ImportError(
                    'msvcrt is required for _get_os_handle on Windows '
                    'but not found.'
                ) from e
            return cast(int, msvcrt.get_osfhandle(fd))  # type: ignore[attr-defined,redundant-cast]

        def lock(self, file_obj: FileArgument, flags: LockFlags) -> None:
            import pywintypes
            import win32con
            import win32file
            import winerror

            fd, io_obj_ctx, pos_ctx = _prepare_windows_file(file_obj)
            os_fh = self._get_os_handle(fd)

            mode = 0
            if flags & LockFlags.NON_BLOCKING:
                mode |= win32con.LOCKFILE_FAIL_IMMEDIATELY
            if flags & LockFlags.EXCLUSIVE:
                mode |= win32con.LOCKFILE_EXCLUSIVE_LOCK

            try:
                win32file.LockFileEx(
                    os_fh, mode, 0, self._lock_bytes_low, self._overlapped
                )
            except pywintypes.error as exc_value:  # type: ignore[misc]
                if exc_value.winerror == winerror.ERROR_LOCK_VIOLATION:
                    raise AlreadyLocked(
                        LockException.LOCK_FAILED,
                        exc_value.strerror,
                        fh=file_obj,  # Pass original file_obj
                    ) from exc_value
                else:
                    raise
            finally:
                _restore_windows_file_pos(io_obj_ctx, pos_ctx)

        def unlock(self, file_obj: FileArgument) -> None:
            import pywintypes
            import win32file
            import winerror

            fd, io_obj_ctx, pos_ctx = _prepare_windows_file(file_obj)
            os_fh = self._get_os_handle(fd)

            try:
                win32file.UnlockFileEx(
                    os_fh, 0, self._lock_bytes_low, self._overlapped
                )
            except pywintypes.error as exc:  # type: ignore[misc]
                if exc.winerror != winerror.ERROR_NOT_LOCKED:
                    raise LockException(
                        LockException.LOCK_FAILED,
                        exc.strerror,
                        fh=file_obj,  # Pass original file_obj
                    ) from exc
            except OSError as exc:
                raise LockException(
                    LockException.LOCK_FAILED,
                    exc.strerror,
                    fh=file_obj,  # Pass original file_obj
                ) from exc
            finally:
                _restore_windows_file_pos(io_obj_ctx, pos_ctx)

    class MsvcrtLocker(BaseLocker):
        _win32_locker: Win32Locker
        _msvcrt_lock_length: int = 0x10000

        def __init__(self) -> None:
            self._win32_locker = Win32Locker()
            try:
                import msvcrt
            except ImportError as e:
                raise ImportError(
                    'msvcrt is required for MsvcrtLocker but not found.'
                ) from e

            attrs = ['LK_LOCK', 'LK_RLCK', 'LK_NBLCK', 'LK_UNLCK', 'LK_NBRLCK']
            defaults = [0, 1, 2, 3, 2]  # LK_NBRLCK often same as LK_NBLCK (2)
            for attr, default_val in zip(attrs, defaults):
                if not hasattr(msvcrt, attr):
                    setattr(msvcrt, attr, default_val)

        def lock(self, file_obj: FileArgument, flags: LockFlags) -> None:
            import msvcrt

            if flags & LockFlags.SHARED:
                win32_api_flags = LockFlags(0)
                if flags & LockFlags.NON_BLOCKING:
                    win32_api_flags |= LockFlags.NON_BLOCKING
                self._win32_locker.lock(file_obj, win32_api_flags)
                return

            fd, io_obj_ctx, pos_ctx = _prepare_windows_file(file_obj)
            mode = (
                msvcrt.LK_NBLCK  # type: ignore[attr-defined]
                if flags & LockFlags.NON_BLOCKING
                else msvcrt.LK_LOCK  # type: ignore[attr-defined]
            )

            try:
                msvcrt.locking(  # type: ignore[attr-defined]
                    fd,
                    mode,
                    self._msvcrt_lock_length,
                )
            except OSError as exc_value:
                if exc_value.errno in (13, 16, 33, 36):
                    raise AlreadyLocked(
                        LockException.LOCK_FAILED,
                        str(exc_value),
                        fh=file_obj,  # Pass original file_obj
                    ) from exc_value
                raise LockException(
                    LockException.LOCK_FAILED,
                    str(exc_value),
                    fh=file_obj,  # Pass original file_obj
                ) from exc_value
            finally:
                _restore_windows_file_pos(io_obj_ctx, pos_ctx)

        def unlock(self, file_obj: FileArgument) -> None:
            import msvcrt

            fd, io_obj_ctx, pos_ctx = _prepare_windows_file(file_obj)
            took_fallback_path = False

            try:
                msvcrt.locking(  # type: ignore[attr-defined]
                    fd,
                    msvcrt.LK_UNLCK,  # type: ignore[attr-defined]
                    self._msvcrt_lock_length,
                )
            except OSError as exc:
                if exc.errno == 13:  # EACCES (Permission denied)
                    took_fallback_path = True
                    # Restore position before calling win32_locker,
                    # as it will re-prepare.
                    _restore_windows_file_pos(io_obj_ctx, pos_ctx)
                    try:
                        self._win32_locker.unlock(
                            file_obj
                        )  # win32_locker handles its own seeking
                    except LockException as win32_exc:
                        raise LockException(
                            LockException.LOCK_FAILED,
                            f'msvcrt unlock failed ({exc.strerror}), and '
                            f'win32 fallback failed ({win32_exc.strerror})',
                            fh=file_obj,
                        ) from win32_exc
                    except Exception as final_exc:
                        raise LockException(
                            LockException.LOCK_FAILED,
                            f'msvcrt unlock failed ({exc.strerror}), and '
                            f'win32 fallback failed with unexpected error: '
                            f'{final_exc!s}',
                            fh=file_obj,
                        ) from final_exc
                else:
                    raise LockException(
                        LockException.LOCK_FAILED,
                        exc.strerror,
                        fh=file_obj,
                    ) from exc
            finally:
                if not took_fallback_path:
                    _restore_windows_file_pos(io_obj_ctx, pos_ctx)

    _locker_instances: dict[type[BaseLocker], BaseLocker] = dict()

    LOCKER = MsvcrtLocker  # type: ignore[reportConstantRedefinition]

    def lock(file: FileArgument, flags: LockFlags) -> None:
        if isinstance(LOCKER, BaseLocker):
            # If LOCKER is a BaseLocker instance, use its lock method
            locker: Callable[[FileArgument, LockFlags], None] = (
                LOCKER.lock
            )
        elif isinstance(LOCKER, tuple):
            locker = LOCKER[0]  # type: ignore[reportUnknownVariableType]
        elif issubclass(LOCKER, BaseLocker):  # type: ignore[unreachable,arg-type]  # pyright: ignore [reportUnnecessaryIsInstance]
            locker_instance = _locker_instances.get(LOCKER)  # type: ignore[arg-type]
            if locker_instance is None:
                # Create an instance of the locker class if not already done
                _locker_instances[LOCKER] = locker_instance = LOCKER()  # type: ignore[ignore,index,call-arg]

            locker = locker_instance.lock
        else:
            raise TypeError(
                f'LOCKER must be a BaseLocker instance, a tuple of lock and '
                f'unlock functions, or a subclass of BaseLocker, '
                f'got {type(LOCKER)}.'
            )

        locker(file, flags)

    def unlock(file: FileArgument) -> None:
        if isinstance(LOCKER, BaseLocker):
            # If LOCKER is a BaseLocker instance, use its lock method
            unlocker: Callable[[FileArgument], None] = LOCKER.unlock
        elif isinstance(LOCKER, tuple):
            unlocker = LOCKER[1]  # type: ignore[reportUnknownVariableType]

        elif issubclass(LOCKER, BaseLocker):  # type: ignore[unreachable,arg-type]  # pyright: ignore [reportUnnecessaryIsInstance]
            locker_instance = _locker_instances.get(LOCKER)  # type: ignore[arg-type]
            if locker_instance is None:
                # Create an instance of the locker class if not already done
                _locker_instances[LOCKER] = locker_instance = LOCKER()  # type: ignore[ignore,index,call-arg]

            unlocker = locker_instance.unlock
        else:
            raise TypeError(
                f'LOCKER must be a BaseLocker instance, a tuple of lock and '
                f'unlock functions, or a subclass of BaseLocker, '
                f'got {type(LOCKER)}.'
            )

        unlocker(file)

else:  # pragma: not-nt
    import errno
    import fcntl

    # PosixLocker methods accept FileArgument | HasFileno
    PosixFileArgument = Union[FileArgument, HasFileno]

    class PosixLocker(BaseLocker):
        """Locker implementation using the `LOCKER` constant"""

        _locker: Optional[
            Callable[[Union[int, HasFileno], int], Any]
        ] = None

        @property
        def locker(self) -> Callable[[Union[int, HasFileno], int], Any]:
            if self._locker is None:
                # On POSIX systems ``LOCKER`` is a callable (fcntl.flock) but
                # mypy also sees the Windows-only tuple assignment.  Explicitly
                # cast so mypy knows we are returning the callable variant
                # here.
                return cast(
                    Callable[[Union[int, HasFileno], int], Any], LOCKER
                )  # pyright: ignore[reportUnnecessaryCast]

            # mypy does not realise ``self._locker`` is non-None after the
            # check
            assert self._locker is not None
            return self._locker

        def _get_fd(self, file_obj: PosixFileArgument) -> int:
            if isinstance(file_obj, int):
                return file_obj
            # Check for fileno() method; covers typing.IO and HasFileno
            elif hasattr(file_obj, 'fileno') and callable(file_obj.fileno):
                return file_obj.fileno()
            else:
                # Should not be reached if PosixFileArgument is correct.
                # isinstance(file_obj, io.IOBase) could be an
                # alternative check
                # but hasattr is more general for HasFileno.
                raise TypeError(
                    "Argument 'file_obj' must be an int, an IO object "
                    'with fileno(), or implement HasFileno.'
                )

        def lock(self, file_obj: PosixFileArgument, flags: LockFlags) -> None:
            if (flags & LockFlags.NON_BLOCKING) and not flags & (
                LockFlags.SHARED | LockFlags.EXCLUSIVE
            ):
                raise RuntimeError(
                    'When locking in non-blocking mode on POSIX, '
                    'the SHARED or EXCLUSIVE flag must be specified as well.'
                )

            fd = self._get_fd(file_obj)
            try:
                self.locker(fd, flags)
            except OSError as exc_value:
                if exc_value.errno in (errno.EACCES, errno.EAGAIN):
                    raise AlreadyLocked(
                        exc_value,
                        strerror=str(exc_value),
                        fh=file_obj,  # Pass original file_obj
                    ) from exc_value
                else:
                    raise LockException(
                        exc_value,
                        strerror=str(exc_value),
                        fh=file_obj,  # Pass original file_obj
                    ) from exc_value
            except EOFError as exc_value:  # NFS specific
                raise LockException(
                    exc_value,
                    strerror=str(exc_value),
                    fh=file_obj,  # Pass original file_obj
                ) from exc_value

        def unlock(self, file_obj: PosixFileArgument) -> None:
            fd = self._get_fd(file_obj)
            self.locker(fd, LockFlags.UNBLOCK)

    class FlockLocker(PosixLocker):
        """FlockLocker is a PosixLocker implementation using fcntl.flock."""

        LOCKER = fcntl.flock  # type: ignore[attr-defined]

    class LockfLocker(PosixLocker):
        """LockfLocker is a PosixLocker implementation using fcntl.lockf."""

        LOCKER = fcntl.lockf  # type: ignore[attr-defined]

    # LOCKER constant for POSIX is fcntl.flock for backward compatibility.
    # Type matches: Callable[[Union[int, HasFileno], int], Any]
    LOCKER = fcntl.flock  # type: ignore[attr-defined,reportConstantRedefinition]

    _posix_locker_instance = PosixLocker()

    # Public API for POSIX uses the PosixLocker instance
    def lock(file: FileArgument, flags: LockFlags) -> None:
        _posix_locker_instance.lock(file, flags)

    def unlock(file: FileArgument) -> None:
        _posix_locker_instance.unlock(file)

import abc
import atexit
import contextlib
import logging
import os
import pathlib
import random
import tempfile
import time
import typing
import warnings

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5
DEFAULT_CHECK_INTERVAL = 0.25
DEFAULT_FAIL_WHEN_LOCKED = False
LOCK_METHOD = LockFlags.EXCLUSIVE | LockFlags.NON_BLOCKING

__all__ = [
    'Lock',
    'open_atomic',
]


def coalesce(*args: typing.Any, test_value: typing.Any = None) -> typing.Any:
    """Simple coalescing function that returns the first value that is not
    equal to the `test_value`. Or `None` if no value is valid. Usually this
    means that the last given value is the default value.

    Note that the `test_value` is compared using an identity check
    (i.e. `value is not test_value`) so changing the `test_value` won't work
    for all values.

    >>> coalesce(None, 1)
    1
    >>> coalesce()

    >>> coalesce(0, False, True)
    0
    >>> coalesce(0, False, True, test_value=0)
    False

    # This won't work because of the `is not test_value` type testing:
    >>> coalesce([], dict(spam='eggs'), test_value=[])
    []
    """
    return next((arg for arg in args if arg is not test_value), None)


@contextlib.contextmanager
def open_atomic(
    filename: Filename,
    binary: bool = True,
) -> typing.Iterator[IO]:
    """Open a file for atomic writing. Instead of locking this method allows
    you to write the entire file and move it to the actual location. Note that
    this makes the assumption that a rename is atomic on your platform which
    is generally the case but not a guarantee.

    http://docs.python.org/library/os.html#os.rename

    >>> filename = 'test_file.txt'
    >>> if os.path.exists(filename):
    ...     os.remove(filename)

    >>> with open_atomic(filename) as fh:
    ...     written = fh.write(b'test')
    >>> assert os.path.exists(filename)
    >>> os.remove(filename)

    >>> import pathlib
    >>> path_filename = pathlib.Path('test_file.txt')

    >>> with open_atomic(path_filename) as fh:
    ...     written = fh.write(b'test')
    >>> assert path_filename.exists()
    >>> path_filename.unlink()
    """
    # `pathlib.Path` cast in case `path` is a `str`
    path: pathlib.Path
    if isinstance(filename, pathlib.Path):
        path = filename
    else:
        path = pathlib.Path(filename)

    assert not path.exists(), f'{path!r} exists'

    # Create the parent directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode=(binary and 'wb') or 'w',
        dir=str(path.parent),
        delete=False,
    ) as temp_fh:
        yield temp_fh
        temp_fh.flush()
        os.fsync(temp_fh.fileno())

    try:
        os.rename(temp_fh.name, path)
    finally:
        with contextlib.suppress(Exception):
            os.remove(temp_fh.name)


class LockBase(abc.ABC):  # pragma: no cover
    #: timeout when trying to acquire a lock
    timeout: float
    #: check interval while waiting for `timeout`
    check_interval: float
    #: skip the timeout and immediately fail if the initial lock fails
    fail_when_locked: bool

    def __init__(
        self,
        timeout: float | None = None,
        check_interval: float | None = None,
        fail_when_locked: bool | None = None,
    ) -> None:
        self.timeout = coalesce(timeout, DEFAULT_TIMEOUT)
        self.check_interval = coalesce(check_interval, DEFAULT_CHECK_INTERVAL)
        self.fail_when_locked = coalesce(
            fail_when_locked,
            DEFAULT_FAIL_WHEN_LOCKED,
        )

    @abc.abstractmethod
    def acquire(
        self,
        timeout: float | None = None,
        check_interval: float | None = None,
        fail_when_locked: bool | None = None,
    ) -> typing.IO[typing.AnyStr]: ...

    def _timeout_generator(
        self,
        timeout: float | None,
        check_interval: float | None,
    ) -> typing.Iterator[int]:
        f_timeout = coalesce(timeout, self.timeout, 0.0)
        f_check_interval = coalesce(check_interval, self.check_interval, 0.0)

        yield 0
        i = 0

        start_time = time.perf_counter()
        while start_time + f_timeout > time.perf_counter():
            i += 1
            yield i

            # Take low lock checks into account to stay within the interval
            since_start_time = time.perf_counter() - start_time
            time.sleep(max(0.001, (i * f_check_interval) - since_start_time))

    @abc.abstractmethod
    def release(self) -> None: ...

    def __enter__(self) -> typing.IO[typing.AnyStr]:
        return self.acquire()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: typing.Any,  # Should be typing.TracebackType
    ) -> bool | None:
        self.release()
        return None

    def __delete__(self, instance: LockBase) -> None:
        instance.release()


class Lock(LockBase):
    """Lock manager with built-in timeout

    Args:
        filename: filename
        mode: the open mode, 'a' or 'ab' should be used for writing. When mode
            contains `w` the file will be truncated to 0 bytes.
        timeout: timeout when trying to acquire a lock
        check_interval: check interval while waiting
        fail_when_locked: after the initial lock failed, return an error
            or lock the file. This does not wait for the timeout.
        **file_open_kwargs: The kwargs for the `open(...)` call

    fail_when_locked is useful when multiple threads/processes can race
    when creating a file. If set to true than the system will wait till
    the lock was acquired and then return an AlreadyLocked exception.

    Note that the file is opened first and locked later. So using 'w' as
    mode will result in truncate _BEFORE_ the lock is checked.
    """

    fh: IO | None
    filename: str
    mode: str
    truncate: bool
    timeout: float
    check_interval: float
    fail_when_locked: bool
    flags: LockFlags
    file_open_kwargs: dict[str, typing.Any]

    def __init__(
        self,
        filename: Filename,
        mode: Mode = 'a',
        timeout: float | None = None,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        fail_when_locked: bool = DEFAULT_FAIL_WHEN_LOCKED,
        flags: LockFlags = LOCK_METHOD,
        **file_open_kwargs: typing.Any,
    ) -> None:
        if 'w' in mode:
            truncate = True
            mode = typing.cast(Mode, mode.replace('w', 'a'))
        else:
            truncate = False

        if timeout is None:
            timeout = DEFAULT_TIMEOUT
        elif not (flags & LockFlags.NON_BLOCKING):
            warnings.warn(
                'timeout has no effect in blocking mode',
                stacklevel=1,
            )

        self.fh = None
        self.filename = str(filename)
        self.mode = mode
        self.truncate = truncate
        self.flags = flags
        self.file_open_kwargs = file_open_kwargs
        super().__init__(timeout, check_interval, fail_when_locked)

    def acquire(
        self,
        timeout: float | None = None,
        check_interval: float | None = None,
        fail_when_locked: bool | None = None,
    ) -> typing.IO[typing.AnyStr]:
        """Acquire the locked filehandle"""

        fail_when_locked = coalesce(fail_when_locked, self.fail_when_locked)

        if (
            not (self.flags & LockFlags.NON_BLOCKING)
            and timeout is not None
        ):
            warnings.warn(
                'timeout has no effect in blocking mode',
                stacklevel=1,
            )

        # If we already have a filehandle, return it
        fh = self.fh
        if fh:
            # Due to type invariance we need to cast the type
            return typing.cast(typing.IO[typing.AnyStr], fh)

        # Get a new filehandler
        fh = self._get_fh()

        def try_close() -> None:  # pragma: no cover
            # Silently try to close the handle if possible, ignore all issues
            if fh is not None:
                with contextlib.suppress(Exception):
                    fh.close()

        exception = None
        # Try till the timeout has passed
        for _ in self._timeout_generator(timeout, check_interval):
            exception = None
            try:
                # Try to lock
                fh = self._get_lock(fh)
                break
            except LockException as exc:
                # Python will automatically remove the variable from memory
                # unless you save it in a different location
                exception = exc

                # We already tried to the get the lock
                # If fail_when_locked is True, stop trying
                if fail_when_locked:
                    try_close()
                    raise AlreadyLocked(exception) from exc
            except Exception as exc:
                # Something went wrong with the locking mechanism.
                # Wrap in a LockException and re-raise:
                try_close()
                raise LockException(exc) from exc

            # Wait a bit

        if exception:
            try_close()
            # We got a timeout... reraising
            raise exception

        # Prepare the filehandle (truncate if needed)
        fh = self._prepare_fh(fh)

        self.fh = fh
        return typing.cast(typing.IO[typing.AnyStr], fh)

    def __enter__(self) -> typing.IO[typing.AnyStr]:
        return self.acquire()

    def release(self) -> None:
        """Releases the currently locked file handle"""
        if self.fh:
            unlock(self.fh)
            self.fh.close()
            self.fh = None

    def _get_fh(self) -> IO:
        """Get a new filehandle"""
        return typing.cast(
            IO,
            open(  # noqa: SIM115
                self.filename,
                self.mode,
                **self.file_open_kwargs,
            ),
        )

    def _get_lock(self, fh: IO) -> IO:
        """
        Try to lock the given filehandle

        returns LockException if it fails"""
        lock(fh, self.flags)
        return fh

    def _prepare_fh(self, fh: IO) -> IO:
        """
        Prepare the filehandle for usage

        If truncate is a number, the file will be truncated to that amount of
        bytes
        """
        if self.truncate:
            fh.seek(0)
            fh.truncate(0)

        return fh


class RLock(Lock):
    """
    A reentrant lock, functions in a similar way to threading.RLock in that it
    can be acquired multiple times.  When the corresponding number of release()
    calls are made the lock will finally release the underlying file lock.
    """

    def __init__(
        self,
        filename: Filename,
        mode: Mode = 'a',
        timeout: float = DEFAULT_TIMEOUT,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        fail_when_locked: bool = False,
        flags: LockFlags = LOCK_METHOD,
    ) -> None:
        super().__init__(
            filename,
            mode,
            timeout,
            check_interval,
            fail_when_locked,
            flags,
        )
        self._acquire_count = 0

    def acquire(
        self,
        timeout: float | None = None,
        check_interval: float | None = None,
        fail_when_locked: bool | None = None,
    ) -> typing.IO[typing.AnyStr]:
        fh: typing.IO[typing.AnyStr]
        if self._acquire_count >= 1:
            fh = typing.cast(typing.IO[typing.AnyStr], self.fh)
        else:
            fh = super().acquire(timeout, check_interval, fail_when_locked)
        self._acquire_count += 1
        assert fh is not None
        return fh

    def release(self) -> None:
        if self._acquire_count == 0:
            raise LockException(
                'Cannot release more times than acquired',
            )

        if self._acquire_count == 1:
            super().release()
        self._acquire_count -= 1


class TemporaryFileLock(Lock):
    def __init__(
        self,
        filename: str = '.lock',
        timeout: float = DEFAULT_TIMEOUT,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        fail_when_locked: bool = True,
        flags: LockFlags = LOCK_METHOD,
    ) -> None:
        super().__init__(
            filename=filename,
            mode='w',
            timeout=timeout,
            check_interval=check_interval,
            fail_when_locked=fail_when_locked,
            flags=flags,
        )
        atexit.register(self.release)

    def release(self) -> None:
        Lock.release(self)
        if os.path.isfile(self.filename):  # pragma: no branch
            os.unlink(self.filename)


class BoundedSemaphore(LockBase):
    """
    Bounded semaphore to prevent too many parallel processes from running

    This method is deprecated because multiple processes that are completely
    unrelated could end up using the same semaphore.  To prevent this,
    use `NamedBoundedSemaphore` instead. The
    `NamedBoundedSemaphore` is a drop-in replacement for this class.

    >>> semaphore = BoundedSemaphore(2, directory='')
    >>> str(semaphore.get_filenames()[0])
    'bounded_semaphore.00.lock'
    >>> str(sorted(semaphore.get_random_filenames())[1])
    'bounded_semaphore.01.lock'
    """

    lock: Lock | None

    def __init__(
        self,
        maximum: int,
        name: str = 'bounded_semaphore',
        filename_pattern: str = '{name}.{number:02d}.lock',
        directory: str = tempfile.gettempdir(),
        timeout: float | None = DEFAULT_TIMEOUT,
        check_interval: float | None = DEFAULT_CHECK_INTERVAL,
        fail_when_locked: bool | None = True,
    ) -> None:
        self.maximum = maximum
        self.name = name
        self.filename_pattern = filename_pattern
        self.directory = directory
        self.lock: Lock | None = None
        super().__init__(
            timeout=timeout,
            check_interval=check_interval,
            fail_when_locked=fail_when_locked,
        )

        if not name or name == 'bounded_semaphore':
            warnings.warn(
                '`BoundedSemaphore` without an explicit `name` '
                'argument is deprecated, use NamedBoundedSemaphore',
                DeprecationWarning,
                stacklevel=1,
            )

    def get_filenames(self) -> typing.Sequence[pathlib.Path]:
        return [self.get_filename(n) for n in range(self.maximum)]

    def get_random_filenames(self) -> typing.Sequence[pathlib.Path]:
        filenames = list(self.get_filenames())
        random.shuffle(filenames)
        return filenames

    def get_filename(self, number: int) -> pathlib.Path:
        return pathlib.Path(self.directory) / self.filename_pattern.format(
            name=self.name,
            number=number,
        )

    def acquire(  # type: ignore[override]
        self,
        timeout: float | None = None,
        check_interval: float | None = None,
        fail_when_locked: bool | None = None,
    ) -> Lock | None:
        assert not self.lock, 'Already locked'

        filenames = self.get_filenames()

        for n in self._timeout_generator(timeout, check_interval):  # pragma:
            logger.debug('trying lock (attempt %d) %r', n, filenames)
            # no branch
            if self.try_lock(filenames):  # pragma: no branch
                return self.lock  # pragma: no cover

        if fail_when_locked := coalesce(
            fail_when_locked,
            self.fail_when_locked,
        ):
            raise AlreadyLocked()

        return None

    def try_lock(self, filenames: typing.Sequence[Filename]) -> bool:
        filename: Filename
        for filename in filenames:
            logger.debug('trying lock for %r', filename)
            self.lock = Lock(filename, fail_when_locked=True)
            try:
                self.lock.acquire()
            except AlreadyLocked:
                self.lock = None
            else:
                logger.debug('locked %r', filename)
                return True

        return False

    def release(self) -> None:  # pragma: no cover
        if self.lock is not None:
            self.lock.release()
            self.lock = None


class NamedBoundedSemaphore(BoundedSemaphore):
    """
    Bounded semaphore to prevent too many parallel processes from running

    It's also possible to specify a timeout when acquiring the lock to wait
    for a resource to become available.  This is very similar to
    `threading.BoundedSemaphore` but works across multiple processes and across
    multiple operating systems.

    Because this works across multiple processes it's important to give the
    semaphore a name.  This name is used to create the lock files.  If you
    don't specify a name, a random name will be generated.  This means that
    you can't use the same semaphore in multiple processes unless you pass the
    semaphore object to the other processes.

    >>> semaphore = NamedBoundedSemaphore(2, name='test')
    >>> str(semaphore.get_filenames()[0])
    '...test.00.lock'

    >>> semaphore = NamedBoundedSemaphore(2)
    >>> 'bounded_semaphore' in str(semaphore.get_filenames()[0])
    True

    """

    def __init__(
        self,
        maximum: int,
        name: str | None = None,
        filename_pattern: str = '{name}.{number:02d}.lock',
        directory: str = tempfile.gettempdir(),
        timeout: float | None = DEFAULT_TIMEOUT,
        check_interval: float | None = DEFAULT_CHECK_INTERVAL,
        fail_when_locked: bool | None = True,
    ) -> None:
        if name is None:
            name = f'bounded_semaphore.{random.randint(0, 1000000):d}'
        super().__init__(
            maximum,
            name,
            filename_pattern,
            directory,
            timeout,
            check_interval,
            fail_when_locked,
        )

try:  # pragma: no cover
    from .redis import RedisLock
except ImportError:  # pragma: no cover
    RedisLock = None  # type: ignore[assignment,misc]


#: The package name on Pypi
#: Current author and maintainer, view the git history for the previous ones
#: Current author's email address
#: Version number
__version__ = '3.2.0'
#: Package description for Pypi
#: Package homepage


#: Exception thrown when the file is already locked by someone else
#: Exception thrown if an error occurred during locking


#: Lock a file. Note that this is an advisory lock on Linux/Unix systems
#: Unlock a file

#: Place an exclusive lock.
#: Only one process may hold an exclusive lock for a given file at a given
#: time.
LOCK_EX: LockFlags = LockFlags.EXCLUSIVE

#: Place a shared lock.
#: More than one process may hold a shared lock for a given file at a given
#: time.
LOCK_SH: LockFlags = LockFlags.SHARED

#: Acquire the lock in a non-blocking fashion.
LOCK_NB: LockFlags = LockFlags.NON_BLOCKING

#: Remove an existing lock held by this process.
LOCK_UN: LockFlags = LockFlags.UNBLOCK

#: Locking flags enum

#: Locking utility class to automatically handle opening with timeouts and
#: context wrappers

__all__ = [
    'LOCK_EX',
    'LOCK_NB',
    'LOCK_SH',
    'LOCK_UN',
    'AlreadyLocked',
    'BoundedSemaphore',
    'Lock',
    'LockException',
    'LockFlags',
    'RLock',
    'RedisLock',
    'TemporaryFileLock',
    'lock',
    'open_atomic',
    'unlock',
]
