"""Windows job objects: end a child and every process it started, grandchildren included.

The process executor puts each child in its own job, created with KILL_ON_JOB_CLOSE. A process
started by a process in the job joins it too, so terminating the job ends the whole tree even after
the child itself exited (the job handle is not a pid, so it cannot be reused). If the parent dies,
Windows closes the handle and ends the tree as well. Off Windows, `create` returns None.

Stdlib only (ctypes). Every Windows call sits under an `if sys.platform == "win32":` statement so
`mypy --strict` checks this module on both Windows and Linux.
"""

from __future__ import annotations

import sys
import threading

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9  # the JOBOBJECTINFOCLASS value
    _PROCESS_SET_QUOTA = 0x0100
    _PROCESS_TERMINATE = 0x0001

    class _BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            (name, ctypes.c_uint64)
            for name in (
                "ReadOperationCount",
                "WriteOperationCount",
                "OtherOperationCount",
                "ReadTransferCount",
                "WriteTransferCount",
                "OtherTransferCount",
            )
        ]

    class _ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimits),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    # HANDLE is pointer-sized: without an explicit restype ctypes returns a C int and truncates it.
    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    _k32.CreateJobObjectW.restype = wintypes.HANDLE
    _k32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    _k32.SetInformationJobObject.restype = wintypes.BOOL
    _k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _k32.OpenProcess.restype = wintypes.HANDLE
    _k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _k32.AssignProcessToJobObject.restype = wintypes.BOOL
    _k32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _k32.TerminateJobObject.restype = wintypes.BOOL
    _k32.CloseHandle.argtypes = [wintypes.HANDLE]
    _k32.CloseHandle.restype = wintypes.BOOL


class Job:
    """One job handle. `terminate` after `close` does nothing: a cancel grace timer can fire
    after the unit's thread closed the job."""

    def __init__(self, handle: int) -> None:
        self._handle: int | None = handle
        self._lock = threading.Lock()

    def terminate(self) -> None:
        with self._lock:
            if self._handle is None:
                return
            if sys.platform == "win32":
                _k32.TerminateJobObject(self._handle, 1)

    def close(self) -> None:
        with self._lock:
            handle, self._handle = self._handle, None
            if handle is None:
                return
            if sys.platform == "win32":
                _k32.CloseHandle(handle)


def create(pid: int) -> Job | None:
    """Put the running process `pid` in a new KILL_ON_JOB_CLOSE job.

    None off Windows or when Windows refuses, so the caller falls back to taskkill. A process the
    child starts before this call escapes the job; the child is `python -m pipeloom run`, which
    spends hundreds of milliseconds importing before user code runs, so that window stays open
    only in theory (Popen does not expose the thread handle that CREATE_SUSPENDED would need).
    """
    if sys.platform == "win32":  # a statement, not an early return: mypy narrows only here
        job = _k32.CreateJobObjectW(None, None)
        if not job:
            return None
        limits = _ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        proc = None
        try:
            if not _k32.SetInformationJobObject(
                job,
                _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                ctypes.byref(limits),
                ctypes.sizeof(limits),
            ):
                raise OSError(ctypes.get_last_error())
            proc = _k32.OpenProcess(_PROCESS_SET_QUOTA | _PROCESS_TERMINATE, False, pid)
            if not proc or not _k32.AssignProcessToJobObject(job, proc):
                raise OSError(ctypes.get_last_error())
        except OSError:
            _k32.CloseHandle(job)
            return None
        finally:
            if proc:
                _k32.CloseHandle(proc)
        return Job(job)
    return None
