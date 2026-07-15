"""The remoroo wrapper policy server (2026-07-15) — OUR server around the VENDOR'S
policy, constructed exactly like the only path the vendor tests (their in-process
infer_test), with the three deploy-server defects fixed at the source:

  1. reset(robo_name) runs AT STARTUP (their handler never calls it — the lazy-reset
     handler patch dies with this file);
  2. inference runs in a thread EXECUTOR, so handshakes never starve behind a 21s
     GPU call (their handler blocks the event loop);
  3. the obs schema is validated with STATED errors before touching the policy.

`remoroo vla wrapper` writes this template into the vendor workdir and points
.remoroo/vla.yaml's module at it — every vendor-checkout sed becomes unnecessary on
the next machine. The manager launches it exactly like the vendor server:
  python -m remoroo_policy_server --model_path <ckpt> --port <port> [extra_args]
"""
from __future__ import annotations

WRAPPER_MODULE = "remoroo_policy_server"

WRAPPER_TEMPLATE = '''"""remoroo wrapper policy server — written by `remoroo vla wrapper`.
Vendor: LingBot-VLA v2 (constructed in-process like the vendor's own infer_test).
robo_name: {robo_name!r}. Regenerate any time; do not hand-edit."""
import argparse
import asyncio
import concurrent.futures
import sys
import traceback
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "deploy"))

import msgpack_numpy                                    # the vendor wire encoding
import websockets.asyncio.server as _ws


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model_path", type=str, required=True)
    ap.add_argument("--port", type=int, default=8791)
    ap.add_argument("--use_length", type=int, default=50)
    ap.add_argument("--use_compile", type=lambda s: s.lower() == "true", default=False)
    args, _unknown = ap.parse_known_args()

    from lingbot_vla_v2_policy import LingbotVLAv2Server   # the vendor's own class

    policy = LingbotVLAv2Server(args.model_path, use_length=args.use_length,
                                chunk_ret=True, use_bf16=True, use_fp32=False,
                                use_compile=args.use_compile)
    policy.reset({robo_name!r})                 # THE fix: transform built at startup
    print(f"remoroo wrapper serving on :{{args.port}} (robo_name={robo_name!r})",
          flush=True)

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)   # single-flight GPU

    async def handler(ws):
        await ws.send(msgpack_numpy.packb({{"server": "remoroo-wrapper",
                                            "robo_name": {robo_name!r}}}))
        loop = asyncio.get_running_loop()
        while True:
            obs = msgpack_numpy.unpackb(await ws.recv())
            missing = [k for k in ("observation.state", "task") if k not in obs]
            if missing:
                await ws.send(f"remoroo wrapper: obs missing keys {{missing}}")
                continue
            try:                                # GPU in the pool: handshakes stay alive
                action = await loop.run_in_executor(pool, policy.infer, obs)
            except Exception:                   # stated, connection survives
                await ws.send("Traceback (wrapper):\\n" + traceback.format_exc())
                continue
            await ws.send(msgpack_numpy.packb(action))

    async def serve():
        async with _ws.serve(handler, "0.0.0.0", args.port,
                             compression=None, max_size=None):
            await asyncio.Future()

    asyncio.run(serve())


if __name__ == "__main__":
    main()
'''


def write_wrapper(workdir: str, robo_name: str) -> str:
    """Materialize the wrapper into the vendor workdir; returns the path."""
    from pathlib import Path

    out = Path(workdir) / f"{WRAPPER_MODULE}.py"
    out.write_text(WRAPPER_TEMPLATE.format(robo_name=robo_name), encoding="utf-8")
    return str(out)
