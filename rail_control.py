#!/usr/bin/env python3
"""긴급 개방 프로토콜 동안 레일 위치를 제어하기 위한 간단한 브릿지 스크립트."""
import argparse
import json
import sys
from contextlib import contextmanager

try:
    from robot_sdk_v13_rail_extended import BridgeClient
except Exception as exc:  # pragma: no cover - 환경에 따라 의존성 누락 가능
    print(json.dumps({
        "ok": False,
        "error": f"bridge_import_failed: {exc}",
    }, ensure_ascii=False))
    sys.exit(1)


@contextmanager
def bridge(host: str):
    client = BridgeClient(host=host)
    try:
        client.__enter__()
        yield client
    finally:
        try:
            client.__exit__(None, None, None)
        except Exception:
            pass


def ensure_position(client, desired: float, speed: float) -> dict:
    result = client.rail_move_to(desired, speed=speed, wait=True)
    if not isinstance(result, dict):
        result = {"ok": True}
    status = client.rail_status()
    if isinstance(status, dict) and "position" in status:
        result.setdefault("position", status.get("position"))
    return result


def ensure_home(client, speed: float) -> dict:
    result = client.rail_home(speed=speed, wait=True)
    if not isinstance(result, dict):
        result = {"ok": True}
    status = client.rail_status()
    if isinstance(status, dict) and "position" in status:
        result.setdefault("position", status.get("position"))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="AAMS 긴급 개방용 레일 제어")
    parser.add_argument("--action", choices=["extend", "home", "position"], required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--position", type=float, default=800.0)
    parser.add_argument("--speed", type=float, default=200.0)
    args = parser.parse_args()

    payload = {"action": args.action, "position": args.position, "speed": args.speed}
    try:
        with bridge(args.host) as client:
            if args.action == "home":
                response = ensure_home(client, args.speed)
            else:
                response = ensure_position(client, args.position, args.speed)
    except Exception as exc:  # pragma: no cover - 하드웨어 환경 의존
        payload.update({
            "ok": False,
            "error": str(exc),
        })
        print(json.dumps(payload, ensure_ascii=False))
        return 1

    payload.update(response or {})
    payload.setdefault("ok", True)
    if payload.get("current_position") and "position" not in payload:
        payload["position"] = payload["current_position"]
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload.get("ok", True) else 1


if __name__ == "__main__":
    sys.exit(main())