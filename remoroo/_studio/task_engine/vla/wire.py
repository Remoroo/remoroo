"""LingBot policy-server wire protocol — WebSocket + msgpack-numpy, mirrored from the
repo's deploy/ client (websocket_client_policy.py + msgpack_numpy.py) so our bytes are
byte-compatible with their server:

  * connect ws://host:port with compression=None, max_size=None; the server sends ONE
    msgpack metadata frame on connect;
  * each inference: send packb(obs), recv packb(action_dict); a STRING response is the
    server reporting an inference error (raise it, stated);
  * numpy arrays travel as {b"__ndarray__": True, b"data": tobytes, b"dtype": str,
    b"shape": tuple} (their exact dict encoding — NOT msgpack ext types).

msgpack + websockets import lazily (GPU cell only); CI injects a fake transport.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import numpy as np


# --- codec: exact mirror of deploy/msgpack_numpy.py ---------------------------------------
def _pack_array(obj: Any) -> Any:
    if isinstance(obj, (np.ndarray, np.generic)) and obj.dtype.kind in ("V", "O", "c"):
        raise ValueError(f"Unsupported dtype: {obj.dtype}")
    if isinstance(obj, np.ndarray):
        return {b"__ndarray__": True, b"data": obj.tobytes(),
                b"dtype": obj.dtype.str, b"shape": obj.shape}
    if isinstance(obj, np.generic):
        return {b"__npgeneric__": True, b"data": obj.item(), b"dtype": obj.dtype.str}
    return obj


def _unpack_array(obj: Dict[Any, Any]) -> Any:
    if b"__ndarray__" in obj:
        return np.ndarray(buffer=obj[b"data"], dtype=np.dtype(obj[b"dtype"]),
                          shape=obj[b"shape"])
    if b"__npgeneric__" in obj:
        return np.dtype(obj[b"dtype"]).type(obj[b"data"])
    return obj


def packb(obj: Any) -> bytes:
    import msgpack

    return msgpack.packb(obj, default=_pack_array)


def unpackb(data: bytes) -> Any:
    import msgpack

    return msgpack.unpackb(data, object_hook=_unpack_array)


# --- client --------------------------------------------------------------------------------
def _default_connect(uri: str) -> Any:
    import websockets.sync.client

    # open_timeout 60s: the vendor server BLOCKS its event loop for the whole ~21s
    # GPU inference (sync infer inside the async handler, rig-measured 2026-07-15),
    # so a legitimate handshake can wait behind an in-flight inference. The default
    # 10s open_timeout made healthy connects "fail" whenever the server was thinking.
    return websockets.sync.client.connect(uri, compression=None, max_size=None,
                                          open_timeout=60)


class PolicyClient:
    """One connection, request/response inference. connect_fn is injectable: tests pass a
    fake with send/recv/close; the rig uses the real websocket."""

    def __init__(self, server_url: str,
                 connect_fn: Optional[Callable[[str], Any]] = None) -> None:
        # accept http://host:port (our older config style) and normalize to ws://
        self.uri = server_url.replace("http://", "ws://").replace("https://", "wss://")
        if not self.uri.startswith(("ws://", "wss://")):
            self.uri = "ws://" + self.uri
        self._connect = connect_fn or _default_connect
        self._ws: Any = None
        self.metadata: Dict[str, Any] = {}

    def _ensure(self) -> None:
        if self._ws is None:
            self._ws = self._connect(self.uri)
            raw = self._ws.recv()                    # the server's hello/metadata frame
            self.metadata = unpackb(raw) if isinstance(raw, (bytes, bytearray)) else {}

    def infer(self, obs: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure()
        self._ws.send(packb(obs))
        response = self._ws.recv()
        if isinstance(response, str):                # server-side inference error, stated
            raise RuntimeError(f"policy server error: {response}")
        return unpackb(response)

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:                        # noqa: BLE001
                pass
            self._ws = None
