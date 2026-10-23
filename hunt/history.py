"""Client histories: what each client asked for, when, and what it was told.

An operation is recorded at invocation and again when the client learns the
outcome. Outcomes come in three kinds, and the checker treats them
differently:

- ``ok``   the operation took effect, and ``output`` is what the client saw.
- ``fail`` the operation definitely did not take effect.
- ``info`` the client doesn't know (timeout, crash, an error that does not
  rule out the command having been committed). It may or may not have taken
  effect, at any point after it was invoked.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Optional

INF = float("inf")


@dataclass
class Op:
    id: int
    client: str
    kind: str                  # "write" | "read" | "cas"
    key: str
    value: Any = None          # write: new value; cas: [expected, new]
    invoke: float = 0.0
    complete: float = INF      # INF while unknown
    status: str = "info"       # "ok" | "fail" | "info"
    output: Any = None         # read: value seen; cas: True/False
    note: str = ""             # e.g. the library's error code

    def to_json(self) -> dict:
        d = asdict(self)
        if d["complete"] == INF:
            d["complete"] = None
        return d

    @staticmethod
    def from_json(d: dict) -> "Op":
        d = dict(d)
        if d.get("complete") is None:
            d["complete"] = INF
        return Op(**d)


@dataclass
class History:
    ops: list[Op] = field(default_factory=list)

    def invoke(self, client: str, kind: str, key: str, value: Any, t: float) -> Op:
        op = Op(id=len(self.ops), client=client, kind=kind, key=key, value=value, invoke=t)
        self.ops.append(op)
        return op

    @staticmethod
    def ok(op: Op, t: float, output: Any = None) -> None:
        op.status, op.complete, op.output = "ok", t, output

    @staticmethod
    def fail(op: Op, t: float, note: str = "") -> None:
        op.status, op.complete, op.note = "fail", t, note

    @staticmethod
    def info(op: Op, note: str = "") -> None:
        op.status, op.complete, op.note = "info", INF, note

    def keys(self) -> list[str]:
        return sorted({o.key for o in self.ops})

    def for_key(self, key: str) -> list[Op]:
        return [o for o in self.ops if o.key == key]

    def dump(self, path: str) -> None:
        with open(path, "w") as f:
            for o in self.ops:
                f.write(json.dumps(o.to_json()) + "\n")

    @staticmethod
    def load(path: str) -> "History":
        with open(path) as f:
            return History([Op.from_json(json.loads(line)) for line in f if line.strip()])

    @staticmethod
    def of(ops: Iterable[Op]) -> "History":
        return History(list(ops))


def unknown(op: Op) -> bool:
    return op.status == "info"


def outcome_known(op: Op) -> Optional[bool]:
    return None if op.status == "info" else op.status == "ok"
