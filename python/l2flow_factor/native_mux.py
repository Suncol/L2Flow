"""Fail-closed seam for native safe-mux/snapshot-as-of proof bindings.

This repository-local Python slice does not claim a ctypes or pybind11 binding
to the C++ proof helpers.  It intentionally contains no Python reimplementation
of their ordering/frontier algorithm.  A deployment must inject a reviewed C
ABI adapter; absent that adapter, every proof request fails closed.
"""

from __future__ import annotations

from typing import Any, Protocol

from .errors import NativeProofUnavailable


class NativeMuxAdapter(Protocol):
    def select_safe_candidate(self, native_request: Any) -> Any:
        ...

    def prove_snapshot_asof_tick(self, native_request: Any) -> Any:
        ...


class FailClosedNativeMux:
    def select_safe_candidate(self, native_request: Any) -> Any:
        del native_request
        raise NativeProofUnavailable(
            "native safe-mux C ABI adapter is not installed; Python proof is forbidden"
        )

    def prove_snapshot_asof_tick(self, native_request: Any) -> Any:
        del native_request
        raise NativeProofUnavailable(
            "native snapshot-as-of C ABI adapter is not installed; Python proof is forbidden"
        )


NATIVE_MUX = FailClosedNativeMux()
