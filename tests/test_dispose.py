"""销毁/报废模块端到端测试:多样品事务销毁、可用量校验、不可变审计、
出库流水与版本递增、幂等重放、批量/dry-run 接入、旧库迁移。"""
from __future__ import annotations

import json
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app import service
from app.db import connect, init_db
from app.main import app, get_conn


@pytest.fixture()
def client():
    mem = connect(":memory:")
    init_db(mem)

    def _override():
        yield mem

    app.dependency_overrides[get_conn] = _override
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
    mem.close()


def _sample(client, op_no, sid, amount, unit="mL"):
    r = client.post("/api/v1/operations/sampling", json={
        "type": "sampling", "op_no": op_no, "sample_id": sid,
        "material": "血清", "amount": amount, "unit": unit,
    })
    assert r.status_code == 200, r.text


def _dispose(client, op_no, items, **extra):
    return client.post("/api/v1/operations/dispose", json={
        "type": "dispose", "op_no": op_no, "items": items, **extra,
    })


# ---------------------------------------------------------------- 基础链路

def test_dispose_multiple_samples_writes_audit_ledger_and_version(client):
    _sample(client, "OP-1", "S1", 100)
    _sample(client, "OP-2", "S2", 50)

    r = _dispose(client, "DSP-1", [
        {"sample_id": "S1", "amount": 30, "reason": "过期报废"},
        {"sample_id": "S2", "amount": 50, "reason": "污染销毁"},
    ], location="销毁间B")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["type"] == "dispose"
    assert body["replayed"] is False
    assert body["samples_created"] == []
    assert body["edges"] == []
    assert body["remaining"] == {"S1": 70.0, "S2": 0.0}
    disposed = {d["sample_id"]: d for d in body["disposed"]}
    assert disposed["S1"] == {
        "sample_id": "S1", "amount": 30.0, "unit": "mL", "reason": "过期报废",
    }
    assert disposed["S2"]["reason"] == "污染销毁"

    # 样品版本递增(每样品一次销毁 +1)
    got = client.get("/api/v1/samples").json()
    ver = {i["sample_id"]: i for i in got["items"]}
    assert ver["S1"]["remaining"] == 70.0
    assert ver["S2"]["remaining"] == 0.0

    # 出库流水 + 销毁审计记录落库;账本重算一致
    r = client.get("/api/v1/checks/balances")
    assert r.json()["consistent"] is True, r.json()
    bal = {b["sample_id"]: b for b in r.json()["samples"]}
    assert bal["S1"]["total_out"] == 30.0
    assert bal["S1"]["stored_remaining"] == 70.0
    assert bal["S2"]["ledger_remaining"] == 0.0

    # 审计记录查询(含操作号/原因/位置)
    r = client.get("/api/v1/disposals")
    assert r.status_code == 200
    assert r.json()["total"] == 2
    rec = {i["sample_id"]: i for i in r.json()["items"]}
    assert rec["S1"]["op_no"] == "DSP-1"
    assert rec["S1"]["reason"] == "过期报废"
    assert rec["S1"]["location"] == "销毁间B"
    assert rec["S2"]["amount"] == 50.0

    r = client.get("/api/v1/disposals?sample_id=S1")
    assert r.json()["total"] == 1
    assert r.json()["items"][0]["sample_id"] == "S1"


def test_dispose_via_generic_operations_endpoint(client):
    _sample(client, "OP-1", "S1", 10)
    r = client.post("/api/v1/operations", json={
        "type": "dispose", "op_no": "DSP-G",
        "items": [{"sample_id": "S1", "amount": 4, "reason": "例行报废"}],
    })
    assert r.status_code == 200, r.text
    assert r.json()["type"] == "dispose"
    assert r.json()["remaining"] == {"S1": 6.0}

    # 按操作号回放可查到销毁明细
    got = client.get("/api/v1/operations/DSP-G").json()
    assert got["type"] == "dispose"
    assert got["effect"]["disposed"][0]["reason"] == "例行报废"


def test_dispose_version_increment_visible_to_stocktake(client):
    """销毁递增样品版本,盘点乐观锁能观察到快照后的销毁变更。"""
    _sample(client, "OP-1", "S1", 100)
    client.post("/api/v1/stocktakes", json={
        "stocktake_no": "STK-D", "items": [{"sample_id": "S1"}],
    })
    _dispose(client, "DSP-1", [
        {"sample_id": "S1", "amount": 40, "reason": "破损"},
    ])
    r = client.post("/api/v1/stocktakes/STK-D/submit", json={
        "items": [{"sample_id": "S1", "counted_amount": 60}],
    })
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "stocktake_version_conflict"


# ---------------------------------------------------------------- 可用量校验

def test_dispose_checks_available_against_active_reservations(client):
    _sample(client, "OP-1", "S1", 100)
    client.post("/api/v1/reservations", json={
        "reservation_no": "RSV-1",
        "items": [{"sample_id": "S1", "amount": 60}],
    })

    # 账面余量 100,但未释放预留 60 -> 可用 40,销毁 50 被拒
    r = _dispose(client, "DSP-1", [
        {"sample_id": "S1", "amount": 50, "reason": "过期"},
    ])
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "insufficient_available"
    assert c["sample_id"] == "S1"
    assert c["op_no"] == "DSP-1"
    assert c["requested"] == 50.0
    assert c["available"] == 40.0

    # 可用量 40 刚好:成功
    r = _dispose(client, "DSP-2", [
        {"sample_id": "S1", "amount": 40, "reason": "过期"},
    ])
    assert r.status_code == 200, r.text
    assert r.json()["remaining"] == {"S1": 60.0}

    # 释放预留后可用量恢复,可继续销毁
    client.delete("/api/v1/reservations/RSV-1")
    r = _dispose(client, "DSP-3", [
        {"sample_id": "S1", "amount": 60, "reason": "过期"},
    ])
    assert r.status_code == 200, r.text
    assert r.json()["remaining"] == {"S1": 0.0}


def test_dispose_partial_shortage_rolls_back_all_items(client):
    _sample(client, "OP-1", "S1", 100)
    _sample(client, "OP-2", "S2", 10)
    _sample(client, "OP-3", "S3", 5)

    # S1 足够、S2 不足、S3 不存在 -> 逐项冲突,整单回滚
    r = _dispose(client, "DSP-BAD", [
        {"sample_id": "S1", "amount": 90, "reason": "过期"},
        {"sample_id": "S2", "amount": 20, "reason": "污染"},
        {"sample_id": "GHOST", "amount": 1, "reason": "丢失"},
    ])
    assert r.status_code == 409
    conflicts = r.json()["conflicts"]
    assert len(conflicts) == 2
    by_sample = {c["sample_id"]: c for c in conflicts}
    assert by_sample["S2"]["code"] == "insufficient_available"
    assert by_sample["S2"]["requested"] == 20.0
    assert by_sample["S2"]["available"] == 10.0
    assert by_sample["GHOST"]["code"] == "sample_not_found"

    # 整体回滚:S1 余量未动,无审计记录/出库流水/操作记录
    got = client.get("/api/v1/samples").json()
    rem = {i["sample_id"]: i["remaining"] for i in got["items"]}
    assert rem == {"S1": 100.0, "S2": 10.0, "S3": 5.0}
    assert client.get("/api/v1/disposals").json()["total"] == 0
    assert client.get("/api/v1/operations/DSP-BAD").status_code == 404
    assert client.get("/api/v1/checks/balances").json()["consistent"] is True


def test_dispose_unknown_sample_only(client):
    r = _dispose(client, "DSP-GHOST", [
        {"sample_id": "NOPE", "amount": 1, "reason": "丢失"},
    ])
    assert r.status_code == 409
    assert r.json()["conflicts"][0]["code"] == "sample_not_found"


def test_dispose_duplicate_sample_rejected(client):
    _sample(client, "OP-1", "S1", 10)
    r = _dispose(client, "DSP-DUP", [
        {"sample_id": "S1", "amount": 1, "reason": "a"},
        {"sample_id": "S1", "amount": 2, "reason": "b"},
    ])
    assert r.status_code == 422


def test_dispose_validation_requires_positive_amount_and_reason(client):
    _sample(client, "OP-1", "S1", 10)
    r = _dispose(client, "DSP-NEG", [
        {"sample_id": "S1", "amount": 0, "reason": "x"},
    ])
    assert r.status_code == 422
    r = _dispose(client, "DSP-NOREASON", [
        {"sample_id": "S1", "amount": 1, "reason": ""},
    ])
    assert r.status_code == 422


# ---------------------------------------------------------------- 幂等

def test_dispose_idempotent_replay_and_payload_conflict(client):
    _sample(client, "OP-1", "S1", 100)
    body = {
        "type": "dispose", "op_no": "DSP-IDEM",
        "items": [{"sample_id": "S1", "amount": 30, "reason": "过期"}],
    }
    r1 = client.post("/api/v1/operations/dispose", json=body)
    assert r1.status_code == 200
    assert r1.json()["replayed"] is False

    # 同号同内容:幂等重放,不重复销毁/写审计
    r2 = client.post("/api/v1/operations/dispose", json=body)
    assert r2.status_code == 200
    assert r2.json()["replayed"] is True
    assert r2.json()["remaining"] == {"S1": 70.0}
    assert r2.json()["disposed"][0]["reason"] == "过期"

    got = client.get("/api/v1/samples").json()
    assert got["items"][0]["remaining"] == 70.0
    assert client.get("/api/v1/disposals").json()["total"] == 1

    # 同号不同内容 -> 409
    r3 = client.post("/api/v1/operations/dispose", json={
        "type": "dispose", "op_no": "DSP-IDEM",
        "items": [{"sample_id": "S1", "amount": 31, "reason": "过期"}],
    })
    assert r3.status_code == 409
    assert r3.json()["conflicts"][0]["code"] == "op_no_conflict"


# ---------------------------------------------------------------- 批量 / dry-run

def test_dispose_in_batch_and_atomic_rollback(client):
    _sample(client, "OP-1", "S1", 100)
    _sample(client, "OP-2", "S2", 50)

    # 批量:分装 + 销毁同一事务提交
    r = client.post("/api/v1/batch", json={
        "batch_id": "B-DSP",
        "operations": [
            {"type": "aliquot", "op_no": "B1", "parent_id": "S1",
             "outputs": [{"sample_id": "A1", "amount": 40}]},
            {"type": "dispose", "op_no": "B2",
             "items": [{"sample_id": "S1", "amount": 20, "reason": "过期"},
                       {"sample_id": "S2", "amount": 50, "reason": "污染"}]},
        ],
    })
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "committed"
    res = r.json()["results"]
    assert res[1]["type"] == "dispose"
    assert res[1]["remaining"] == {"S1": 40.0, "S2": 0.0}
    assert len(res[1]["disposed"]) == 2

    # 批量中销毁不足 -> 整批回滚(分装也不生效),冲突带批内下标
    r = client.post("/api/v1/batch", json={
        "operations": [
            {"type": "aliquot", "op_no": "B3", "parent_id": "S1",
             "outputs": [{"sample_id": "A2", "amount": 10}]},
            {"type": "dispose", "op_no": "B4",
             "items": [{"sample_id": "S1", "amount": 999, "reason": "x"}]},
        ],
    })
    assert r.status_code == 409
    c = r.json()["conflicts"][0]
    assert c["code"] == "insufficient_available"
    assert c["batch_index"] == 1
    assert c["op_no"] == "B4"
    ids = {i["sample_id"] for i in client.get("/api/v1/samples").json()["items"]}
    assert "A2" not in ids  # 分装被回滚


def test_dispose_dry_run_validate_does_not_write(client):
    _sample(client, "OP-1", "S1", 100)

    # 单操作 dry-run:可用量不足
    r = client.post("/api/v1/validate", json={
        "type": "dispose", "op_no": "V-DSP",
        "items": [{"sample_id": "S1", "amount": 101, "reason": "过期"}],
    })
    assert r.status_code == 200
    data = r.json()
    assert data["valid"] is False
    assert data["conflicts"][0]["code"] == "insufficient_available"

    # 批量 dry-run:合法,返回 would_commit_status,不写库
    r = client.post("/api/v1/batch/validate", json={"operations": [
        {"type": "dispose", "op_no": "V-DSP2",
         "items": [{"sample_id": "S1", "amount": 60, "reason": "过期"}]},
    ]})
    data = r.json()
    assert data["valid"] is True
    assert data["would_commit_status"] == "committed"
    assert data["results"][0]["remaining"] == {"S1": 40.0}

    # dry-run 未写库:余量不变、无审计记录
    got = client.get("/api/v1/samples").json()
    assert got["items"][0]["remaining"] == 100.0
    assert client.get("/api/v1/disposals").json()["total"] == 0


# ---------------------------------------------------------------- 审计不可变 / 流水

def test_dispose_records_are_immutable_and_ledger_out(tmp_path):
    """直接对文件库断言:审计记录不可 UPDATE/DELETE,出库流水与版本正确。"""
    db_path = str(tmp_path / "d.db")
    conn = connect(db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, is_root) VALUES ('S1', 'M', 'mL', 100, 100, 1)"
    )
    conn.commit()

    from app.schemas import DisposeOp

    results = service.submit_batch(conn, [DisposeOp(
        type="dispose", op_no="DSP-DB",
        items=[{"sample_id": "S1", "amount": 25, "reason": "过期报废"}],  # type: ignore[arg-type]
    )])
    assert results[0].remaining == {"S1": 75.0}

    row = conn.execute(
        "SELECT remaining, version FROM samples WHERE sample_id = 'S1'"
    ).fetchone()
    assert row["remaining"] == 75.0
    assert row["version"] == 1

    # 出库流水:direction=out,无谱系边
    led = conn.execute(
        "SELECT direction, amount, edge_id, ref_no FROM ledger_entries"
        " WHERE sample_id = 'S1' AND direction = 'out'"
    ).fetchone()
    assert led["direction"] == "out"
    assert led["amount"] == 25.0
    assert led["edge_id"] is None
    assert led["ref_no"] == "DSP-DB"

    rec = conn.execute(
        "SELECT * FROM dispose_records WHERE op_no = 'DSP-DB'"
    ).fetchone()
    assert rec["sample_id"] == "S1"
    assert rec["reason"] == "过期报废"

    # 不可变:UPDATE / DELETE 均被触发器拒绝
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE dispose_records SET amount = 1 WHERE op_no = 'DSP-DB'"
        )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM dispose_records WHERE op_no = 'DSP-DB'")
    conn.rollback()

    # 账本重算一致(根样品销毁后仍对齐)
    bal = service.get_balance(conn, "S1", 1e-6)
    assert bal.consistent is True
    assert bal.stored_remaining == 75.0
    assert bal.ledger_remaining == 75.0
    conn.close()


# ---------------------------------------------------------------- 并发

def test_concurrent_disposes_serialize_without_oversell(tmp_path):
    """两个线程并发销毁同一样品(各 60,共 120 > 100),恰好一个失败。"""
    db_path = str(tmp_path / "c.db")
    setup = connect(db_path)
    init_db(setup)
    setup.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, is_root) VALUES ('S1', 'M', 'mL', 100, 100, 1)"
    )
    setup.commit()
    setup.close()

    from app.schemas import DisposeOp

    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def worker(no: str) -> None:
        c = connect(db_path)
        barrier.wait()
        try:
            service.submit_batch(c, [DisposeOp(
                type="dispose", op_no=no,
                items=[{"sample_id": "S1", "amount": 60, "reason": "并发"}],  # type: ignore[arg-type]
            )])
            outcomes.append("ok")
        except service.ConflictError:
            outcomes.append("conflict")
        finally:
            c.close()

    t1 = threading.Thread(target=worker, args=("DC-1",))
    t2 = threading.Thread(target=worker, args=("DC-2",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert sorted(outcomes) == ["conflict", "ok"]
    check = connect(db_path)
    row = check.execute(
        "SELECT remaining, version FROM samples WHERE sample_id = 'S1'"
    ).fetchone()
    assert row["remaining"] == 40.0
    assert row["version"] == 1
    n = check.execute(
        "SELECT COUNT(*) AS c FROM dispose_records"
    ).fetchone()["c"]
    assert n == 1
    check.close()


# ---------------------------------------------------------------- 旧库迁移

def test_migration_allows_dispose_on_legacy_operations_table(tmp_path):
    """旧库 operations 表 CHECK 不含 'dispose':init_db 重建后销毁全流程可用。"""
    db_path = str(tmp_path / "legacy.db")
    raw = sqlite3.connect(db_path)
    raw.execute(
        "CREATE TABLE operations ("
        " op_id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " op_no TEXT NOT NULL UNIQUE,"
        " op_type TEXT NOT NULL CHECK (op_type IN ('sampling', 'aliquot', 'merge')),"
        " payload TEXT NOT NULL, effect TEXT NOT NULL, seq_index INTEGER NOT NULL,"
        " batch_id TEXT, location TEXT,"
        " created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))"
    )
    raw.execute(
        "INSERT INTO operations (op_no, op_type, payload, effect, seq_index)"
        " VALUES ('OLD-1', 'sampling', '{}',"
        " '{\"samples_created\": [], \"edges\": [], \"remaining\": {}}', 0)"
    )
    raw.execute(
        "CREATE TABLE samples (sample_id TEXT PRIMARY KEY, material TEXT NOT NULL,"
        " unit TEXT NOT NULL, initial_amount REAL NOT NULL, remaining REAL NOT NULL,"
        " location TEXT, is_root INTEGER NOT NULL DEFAULT 0, created_at TEXT)"
    )
    raw.execute(
        "INSERT INTO samples (sample_id, material, unit, initial_amount,"
        " remaining, is_root) VALUES ('S1', 'M', 'mL', 100, 100, 1)"
    )
    raw.commit()
    raw.close()

    conn = connect(db_path)
    init_db(conn)

    # 旧操作记录保留,op_id 不变
    row = conn.execute(
        "SELECT op_id, op_no FROM operations WHERE op_no = 'OLD-1'"
    ).fetchone()
    assert row["op_no"] == "OLD-1"
    old_op_id = row["op_id"]

    # 迁移登记只执行一次:再次 init_db 不报错、不重复
    init_db(conn)
    names = [
        r["name"] for r in conn.execute("SELECT name FROM schema_migrations")
    ]
    assert "operations_allow_dispose" in names

    # 销毁可用
    from app.schemas import DisposeOp

    results = service.submit_batch(conn, [DisposeOp(
        type="dispose", op_no="DSP-M",
        items=[{"sample_id": "S1", "amount": 40, "reason": "迁移后销毁"}],  # type: ignore[arg-type]
    )])
    assert results[0].remaining == {"S1": 60.0}
    row = conn.execute(
        "SELECT remaining, version FROM samples WHERE sample_id = 'S1'"
    ).fetchone()
    assert row["remaining"] == 60.0
    assert row["version"] == 1
    rec = conn.execute(
        "SELECT reason FROM dispose_records WHERE op_no = 'DSP-M'"
    ).fetchone()
    assert rec["reason"] == "迁移后销毁"
    # 旧操作仍在,且新操作 op_id 不复用旧值
    ids = [r["op_id"] for r in conn.execute("SELECT op_id FROM operations")]
    assert old_op_id in ids and len(ids) == 2
    conn.close()
