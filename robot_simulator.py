#!/usr/bin/env python3
"""
robot_simulator.py — AAMS 장비 제어 모의 스크립트

표준 입력으로 JSON 페이로드를 받아 단계별 진행 상황을 표준 출력으로
전달한다. 실제 로봇/레일/비전 제어 코드가 준비되기 전까지 가상의
흐름을 검증하기 위한 목적으로 작성되었다.

출력 형식은 줄 단위 JSON이며, 각 줄은 다음과 같다.
  * {"event": "progress", "stage": "prepare", "message": "..."}
  * {"event": "complete", "status": "success", ...}

스테이지/메시지는 payload에 포함된 요청 정보 및 simulate 파라미터에 따라
달라지며, simulate.fail_stage가 지정되면 해당 단계에서 오류를 발생시켜
실패 흐름을 재현한다.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from typing import Any, Dict

STAGES_RETURN = [
    ("prepare", "반납 준비 중"),
    ("verify", "반납 물품 시각 검사"),
    ("stow", "보관 구역으로 이동"),
    ("complete", "보관 완료"),
]

STAGES_DISPATCH = [
    ("prepare", "불출 준비 중"),
    ("pick", "요청 장비 수거"),
    ("verify", "출고 전 시각 검사"),
    ("handover", "사용자에게 전달"),
]

VISION_CHECKS = [
    ("magazine_top", "탄창 최상단 탄알 위치 확인"),
    ("selector", "조정간 위치 확인"),
    ("serial", "총기 QR 코드 확인"),
]


def emit(event: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def load_payload() -> Dict[str, Any]:
    raw = sys.stdin.read()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        emit({
            "event": "complete",
            "status": "error",
            "stage": "payload",
            "message": f"유효하지 않은 JSON 입력: {exc}"})
        sys.exit(1)


def simulate_delay(min_ms: int = 200, max_ms: int = 900) -> None:
    time.sleep(random.uniform(min_ms, max_ms) / 1000.0)


def run_checks(mode: str, simulate: Dict[str, Any]) -> None:
    if mode != "return":
        return
    for key, label in VISION_CHECKS:
        emit({"event": "progress", "stage": key, "message": label})
        simulate_delay(250, 650)
        if simulate.get("fail_stage") == key:
            raise RuntimeError(simulate.get("reason") or f"{label} 실패")


def main() -> int:
    parser = argparse.ArgumentParser(description="AAMS 로봇/레일 모의 실행기")
    parser.add_argument("--seed", type=int, default=None, help="random seed")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    payload = load_payload()
    request_id = payload.get("requestId") or payload.get("request_id")
    mode = str(payload.get("mode") or "").lower()
    if mode not in {"issue", "dispatch", "return"}:
        mode = "issue" if payload.get("type") in {"DISPATCH", "ISSUE"} else "return"
    mode = "dispatch" if mode in {"issue", "dispatch", "out", "불출"} else "return"

    simulate_cfg = payload.get("simulate") or {}
    stages = STAGES_DISPATCH if mode == "dispatch" else STAGES_RETURN

    emit({
        "event": "progress",
        "stage": "starting",
        "message": f"요청 {request_id or '-'} 장비 명령 준비",
        "mode": mode,
    })
    simulate_delay(200, 500)

    try:
        for stage, label in stages:
            emit({"event": "progress", "stage": stage, "message": label, "mode": mode})
            simulate_delay(400, 900)
            if simulate_cfg.get("fail_stage") == stage:
                raise RuntimeError(simulate_cfg.get("reason") or f"{label} 실패")
        run_checks(mode, simulate_cfg)
    except RuntimeError as exc:
        emit({
            "event": "complete",
            "status": "error",
            "stage": simulate_cfg.get("fail_stage") or "unknown",
            "message": str(exc),
            "mode": mode,
        })
        return 2

    emit({
        "event": "complete",
        "status": "success",
        "stage": "complete",
        "message": "장비 동작 시뮬레이션 완료",
        "mode": mode,
        "summary": {
            "requestId": request_id,
            "includes": payload.get("dispatch", {}).get("includes"),
        },
    })
    return 0


if __name__ == "__main__":
    sys.exit(main())