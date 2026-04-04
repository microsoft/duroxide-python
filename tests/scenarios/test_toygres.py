"""
Toygres Scenario Tests — duroxide Python SDK

Models the toygres PostgreSQL control plane orchestrations. Demonstrates:
  - Instance lifecycle (create → health monitor → delete)
  - Sub-orchestrations (create-instance spawns instance-actor)
  - Eternal orchestrations with continue-as-new (instance-actor, system-pruner)
  - External events for graceful shutdown signaling
  - Race/select between timer and event
  - Retry policies on flaky activities
  - Backup/restore workflows with polling loops

Activities are mocked — no real K8s or CMS. The focus is on orchestration
logic and durable patterns.

Ported from duroxide-node/__tests__/scenarios/toygres-node.test.js
"""

import json
import os
import time
import pytest

from dotenv import load_dotenv

from duroxide import (
    PostgresProvider,
    Client,
    Runtime,
    RuntimeOptions,
)

# Load .env from project root
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

SCHEMA = "duroxide_python_toygres"
RUN_ID = f"tg{int(time.time() * 1000):x}"


def uid(name):
    return f"{RUN_ID}-{name}"


@pytest.fixture(scope="module")
def provider():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        pytest.skip("DATABASE_URL not set")
    return PostgresProvider.connect_with_schema(db_url, SCHEMA)


def create_cms_state():
    """Simulated CMS state shared across activities within a test."""
    return {
        "instances": {},
        "health_checks": [],
        "images": {},
        "dns_names": set(),
        "actors": {},
    }


# ─── 1. Create Instance ──────────────────────────────────────────


def test_create_instance(provider):
    """Provisions a new PostgreSQL instance end-to-end."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))
    cms = create_cms_state()
    ready_attempts = {"count": 0}

    # ── Activities ──

    @runtime.register_activity("cms-create-instance-record")
    def cms_create(ctx, input):
        ctx.trace_info(f"creating CMS record for {input['name']}")
        record = {
            "id": f"inst-{input['name']}",
            "name": input["name"],
            "k8sName": f"pg-{input['name']}",
            "state": "provisioning",
            "dnsName": f"{input['name']}.postgres.local",
        }
        cms["instances"][record["id"]] = record
        cms["dns_names"].add(record["dnsName"])
        return record

    @runtime.register_activity("deploy-postgres-v2")
    def deploy(ctx, input):
        ctx.trace_info(f"deploying K8s resources for {input['k8sName']}")
        return {"statefulSet": input["k8sName"], "service": f"{input['k8sName']}-svc"}

    @runtime.register_activity("wait-for-ready")
    def wait_ready(ctx, input):
        ready_attempts["count"] += 1
        ctx.trace_info(f"checking pod readiness (attempt {ready_attempts['count']})")
        if ready_attempts["count"] < 2:
            return {"ready": False}
        return {"ready": True}

    @runtime.register_activity("get-connection-strings")
    def get_conn(ctx, input):
        ctx.trace_info(f"fetching connection strings for {input['k8sName']}")
        return {
            "host": f"{input['k8sName']}.default.svc.cluster.local",
            "port": 5432,
            "connectionString": f"postgresql://postgres:secret@{input['k8sName']}:5432/postgres",
        }

    @runtime.register_activity("test-connection")
    def test_conn(ctx, input):
        ctx.trace_info(f"testing connection to {input['host']}")
        return {"connected": True, "latencyMs": 3}

    @runtime.register_activity("cms-update-instance-state")
    def update_state(ctx, input):
        ctx.trace_info(f"updating {input['id']} state → {input['state']}")
        inst = cms["instances"].get(input["id"])
        if inst:
            inst["state"] = input["state"]
        return {"ok": True}

    @runtime.register_activity("cms-record-instance-actor")
    def record_actor(ctx, input):
        ctx.trace_info(f"recording actor {input['actorInstanceId']} for {input['id']}")
        inst = cms["instances"].get(input["id"])
        if inst:
            inst["actorInstanceId"] = input["actorInstanceId"]
        return {"ok": True}

    @runtime.register_activity("delete-postgres")
    def delete_pg(ctx, input):
        ctx.trace_info(f"deleting K8s resources for {input['k8sName']}")
        return {"deleted": True}

    # ── Orchestrations ──

    @runtime.register_orchestration("InstanceActor")
    def instance_actor(ctx, input):
        ctx.trace_info(f"actor started for {input['instanceId']}")
        return {"status": "started"}

    @runtime.register_orchestration("CreateInstance")
    def create_instance(ctx, input):
        ctx.trace_info(f"[v1.0.6] creating instance {input['name']}")

        # Step 1: Reserve CMS record
        record = yield ctx.schedule_activity("cms-create-instance-record", {
            "name": input["name"],
            "sku": input.get("sku", "basic"),
        })

        try:
            # Step 2: Deploy K8s resources
            yield ctx.schedule_activity("deploy-postgres-v2", {
                "k8sName": record["k8sName"],
                "sku": input.get("sku", "basic"),
            })

            # Step 3: Wait for pod readiness (polling loop)
            ready = False
            for attempt in range(60):
                status = yield ctx.schedule_activity("wait-for-ready", {
                    "k8sName": record["k8sName"],
                })
                if status["ready"]:
                    ready = True
                    break
                ctx.trace_info(f"pod not ready, waiting (attempt {attempt + 1})")
                yield ctx.schedule_timer(100)

            if not ready:
                raise Exception("pod readiness timeout")

            # Step 4: Get connection info & test
            conn_info = yield ctx.schedule_activity("get-connection-strings", {
                "k8sName": record["k8sName"],
            })

            yield ctx.schedule_activity("test-connection", {
                "host": conn_info["host"],
                "port": conn_info["port"],
            })

            # Step 5: Mark running
            yield ctx.schedule_activity("cms-update-instance-state", {
                "id": record["id"],
                "state": "running",
            })

            # Step 6: Spawn instance actor (fire-and-forget)
            actor_id = f"actor-{record['id']}"
            yield ctx.start_orchestration("InstanceActor", actor_id, {
                "instanceId": record["id"],
                "iteration": 0,
            })

            yield ctx.schedule_activity("cms-record-instance-actor", {
                "id": record["id"],
                "actorInstanceId": actor_id,
            })

            ctx.trace_info(f"instance {input['name']} created successfully")

            return {
                "status": "created",
                "instanceId": record["id"],
                "actorId": actor_id,
                "connectionString": conn_info["connectionString"],
            }
        except Exception as e:
            ctx.trace_error(f"creation failed: {e}")
            yield ctx.schedule_activity("cms-update-instance-state", {
                "id": record["id"],
                "state": "failed",
            })
            yield ctx.schedule_activity("delete-postgres", {
                "k8sName": record["k8sName"],
            })
            return {"status": "failed", "reason": str(e), "instanceId": record["id"]}

    runtime.start()

    try:
        client.start_orchestration(uid("create-mydb"), "CreateInstance", {
            "name": "mydb",
            "sku": "basic",
        })
        result = client.wait_for_orchestration(uid("create-mydb"), 15_000)

        assert result.status == "Completed"
        assert result.output["status"] == "created"
        assert result.output["instanceId"] == "inst-mydb"
        assert result.output["actorId"] == "actor-inst-mydb"
        assert "mydb" in result.output["connectionString"]

        # Verify CMS state
        assert cms["instances"]["inst-mydb"]["state"] == "running"
        assert cms["instances"]["inst-mydb"]["actorInstanceId"] == "actor-inst-mydb"
        assert ready_attempts["count"] >= 2, "should have polled readiness at least twice"
    finally:
        runtime.shutdown(100)


# ─── 2. Instance Actor (health monitor) ──────────────────────────


def test_instance_actor(provider):
    """Runs health checks and exits on instance deleted state."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))
    cms = create_cms_state()
    health_check_count = {"value": 0}

    cms["instances"]["inst-testdb"] = {
        "id": "inst-testdb",
        "name": "testdb",
        "state": "running",
        "connectionString": "postgresql://postgres:secret@pg-testdb:5432/postgres",
    }

    # ── Activities ──

    @runtime.register_activity("cms-get-instance-connection")
    def get_conn(ctx, input):
        ctx.trace_info(f"fetching connection for {input['instanceId']}")
        inst = cms["instances"].get(input["instanceId"])
        if not inst:
            return {"found": False}
        return {"found": True, "connectionString": inst["connectionString"], "state": inst["state"]}

    @runtime.register_activity("test-connection")
    def test_conn(ctx, input):
        return {"connected": True, "latencyMs": 3}

    @runtime.register_activity("cms-record-health-check")
    def record_hc(ctx, input):
        health_check_count["value"] += 1
        ctx.trace_info(f"recording health check #{health_check_count['value']}")
        cms["health_checks"].append({
            "instanceId": input["instanceId"],
            "healthy": input["healthy"],
            "latencyMs": input["latencyMs"],
        })
        return {"ok": True}

    # ── Orchestration ──

    @runtime.register_orchestration("InstanceActor")
    def instance_actor(ctx, input):
        iteration = input.get("iteration", 0)
        ctx.trace_info(f"[v1.0.1] actor iteration {iteration} for {input['instanceId']}")

        conn_info = yield ctx.schedule_activity("cms-get-instance-connection", {
            "instanceId": input["instanceId"],
        })

        if not conn_info["found"] or conn_info["state"] == "deleted":
            ctx.trace_info("instance gone, actor exiting")
            return {"status": "exited", "reason": "instance_not_found", "iterations": iteration}

        health = yield ctx.schedule_activity("test-connection", {
            "connectionString": conn_info["connectionString"],
        })

        yield ctx.schedule_activity("cms-record-health-check", {
            "instanceId": input["instanceId"],
            "healthy": health["connected"],
            "latencyMs": health["latencyMs"],
        })

        # Wait: 100ms timer OR InstanceDeleted event
        winner = yield ctx.race(
            ctx.schedule_timer(100),
            ctx.wait_for_event("InstanceDeleted"),
        )

        if winner["index"] == 1:
            ctx.trace_info("received InstanceDeleted signal, exiting")
            return {"status": "exited", "reason": "signal", "iterations": iteration + 1}

        # continue-as-new for next health check cycle
        yield ctx.continue_as_new({
            "instanceId": input["instanceId"],
            "iteration": iteration + 1,
        })

    runtime.start()

    try:
        client.start_orchestration(uid("actor-testdb"), "InstanceActor", {
            "instanceId": "inst-testdb",
            "iteration": 0,
        })

        # Let it run a few health check cycles
        time.sleep(0.8)

        # Mark instance as deleted so actor exits on next check
        cms["instances"]["inst-testdb"]["state"] = "deleted"

        result = client.wait_for_orchestration(uid("actor-testdb"), 10_000)

        assert result.status == "Completed"
        assert result.output["status"] == "exited"
        assert result.output["reason"] == "instance_not_found"
        assert result.output["iterations"] >= 2, (
            f"expected ≥2 iterations, got {result.output['iterations']}"
        )
        assert health_check_count["value"] >= 2, (
            f"expected ≥2 health checks, got {health_check_count['value']}"
        )
    finally:
        runtime.shutdown(100)


# ─── 3. Delete Instance ──────────────────────────────────────────


def test_delete_instance(provider):
    """Tears down instance and signals actor."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))
    cms = create_cms_state()

    cms["instances"]["inst-delme"] = {
        "id": "inst-delme",
        "name": "delme",
        "k8sName": "pg-delme",
        "state": "running",
        "dnsName": "delme.postgres.local",
        "actorInstanceId": "actor-delme",
    }
    cms["dns_names"].add("delme.postgres.local")

    # ── Activities ──

    @runtime.register_activity("cms-get-instance-by-k8s-name")
    def get_by_k8s(ctx, input):
        ctx.trace_info(f"looking up instance {input['k8sName']}")
        for inst in cms["instances"].values():
            if inst.get("k8sName") == input["k8sName"]:
                return inst
        return None

    @runtime.register_activity("cms-update-instance-state")
    def update_state(ctx, input):
        ctx.trace_info(f"updating {input['id']} → {input['state']}")
        inst = cms["instances"].get(input["id"])
        if inst:
            inst["state"] = input["state"]
        return {"ok": True}

    @runtime.register_activity("send-external-event")
    def send_event(ctx, input):
        ctx.trace_info(f"sending {input['eventName']} to {input['instanceId']}")
        return {"sent": True}

    @runtime.register_activity("delete-postgres")
    def delete_pg(ctx, input):
        ctx.trace_info(f"deleting K8s resources for {input['k8sName']}")
        return {"deleted": True}

    @runtime.register_activity("cms-delete-instance-record")
    def delete_record(ctx, input):
        ctx.trace_info(f"deleting CMS record {input['id']}")
        cms["instances"].pop(input["id"], None)
        return {"ok": True}

    @runtime.register_activity("cms-free-dns-name")
    def free_dns(ctx, input):
        ctx.trace_info(f"freeing DNS name {input['dnsName']}")
        cms["dns_names"].discard(input["dnsName"])
        return {"ok": True}

    # ── Orchestration ──

    @runtime.register_orchestration("DeleteInstance")
    def delete_instance(ctx, input):
        ctx.trace_info(f"[v1.0.2] deleting instance {input['k8sName']}")

        instance = yield ctx.schedule_activity("cms-get-instance-by-k8s-name", {
            "k8sName": input["k8sName"],
        })

        if not instance:
            ctx.trace_warn("instance not found, nothing to delete")
            return {"status": "not_found"}

        yield ctx.schedule_activity("cms-update-instance-state", {
            "id": instance["id"],
            "state": "deleting",
        })

        # Signal actor to stop (best-effort)
        if instance.get("actorInstanceId"):
            try:
                yield ctx.schedule_activity("send-external-event", {
                    "instanceId": instance["actorInstanceId"],
                    "eventName": "InstanceDeleted",
                    "data": {"reason": "instance-deleted"},
                })
            except Exception as e:
                ctx.trace_warn(f"failed to signal actor (best-effort): {e}")

        yield ctx.schedule_activity("delete-postgres", {
            "k8sName": instance["k8sName"],
        })

        yield ctx.schedule_activity("cms-free-dns-name", {
            "dnsName": instance["dnsName"],
        })

        yield ctx.schedule_activity("cms-delete-instance-record", {
            "id": instance["id"],
        })

        ctx.trace_info(f"instance {input['k8sName']} deleted")
        return {"status": "deleted", "instanceId": instance["id"]}

    runtime.start()

    try:
        client.start_orchestration(uid("delete-delme"), "DeleteInstance", {
            "k8sName": "pg-delme",
        })
        result = client.wait_for_orchestration(uid("delete-delme"), 10_000)

        assert result.status == "Completed"
        assert result.output["status"] == "deleted"
        assert result.output["instanceId"] == "inst-delme"

        # CMS should be clean
        assert len(cms["instances"]) == 0
        assert len(cms["dns_names"]) == 0
    finally:
        runtime.shutdown(100)


# ─── 4. Create Image (backup) ────────────────────────────────────


def test_create_image(provider):
    """Runs a backup job and polls to completion."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))
    cms = create_cms_state()
    job_poll_count = {"value": 0}

    cms["instances"]["inst-srcdb"] = {
        "id": "inst-srcdb",
        "name": "srcdb",
        "k8sName": "pg-srcdb",
        "state": "running",
    }

    # ── Activities ──

    @runtime.register_activity("cms-get-instance-by-k8s-name")
    def get_by_k8s(ctx, input):
        for inst in cms["instances"].values():
            if inst.get("k8sName") == input["k8sName"]:
                return inst
        return None

    @runtime.register_activity("cms-get-instance-password")
    def get_pw(ctx, input):
        ctx.trace_info(f"fetching password for {input['k8sName']}")
        return {"password": "secret123"}

    @runtime.register_activity("cms-image-create")
    def img_create(ctx, input):
        ctx.trace_info(f"creating image record: {input['imageName']}")
        image = {
            "id": f"img-{input['imageName']}",
            "name": input["imageName"],
            "state": "pending",
            "blobPath": input["blobPath"],
        }
        cms["images"][image["id"]] = image
        return image

    @runtime.register_activity("run-backup-job")
    def run_backup(ctx, input):
        ctx.trace_info(f"starting backup job for {input['imageName']}")
        return {"jobName": f"backup-{input['imageName']}"}

    @runtime.register_activity("wait-for-job")
    def wait_job(ctx, input):
        job_poll_count["value"] += 1
        ctx.trace_info(f"polling job {input['jobName']} (poll #{job_poll_count['value']})")
        if job_poll_count["value"] < 3:
            return {"status": "running"}
        return {"status": "succeeded"}

    @runtime.register_activity("cms-image-update-state")
    def img_update(ctx, input):
        ctx.trace_info(f"image {input['imageId']} → {input['state']}")
        img = cms["images"].get(input["imageId"])
        if img:
            img["state"] = input["state"]
        return {"ok": True}

    @runtime.register_activity("delete-job")
    def del_job(ctx, input):
        ctx.trace_info(f"cleaning up job {input['jobName']}")
        return {"ok": True}

    # ── Orchestration ──

    @runtime.register_orchestration("CreateImage")
    def create_image(ctx, input):
        ctx.trace_info(f"creating backup image from {input['k8sName']}")

        instance = yield ctx.schedule_activity("cms-get-instance-by-k8s-name", {
            "k8sName": input["k8sName"],
        })
        if not instance:
            return {"status": "failed", "reason": "source not found"}

        pw_result = yield ctx.schedule_activity("cms-get-instance-password", {
            "k8sName": input["k8sName"],
        })
        password = pw_result["password"]

        now = yield ctx.utc_now()
        blob_path = f"images/{input['imageName']}-{now}/"

        image = yield ctx.schedule_activity("cms-image-create", {
            "imageName": input["imageName"],
            "sourceInstanceId": instance["id"],
            "blobPath": blob_path,
        })

        job_result = yield ctx.schedule_activity("run-backup-job", {
            "imageName": input["imageName"],
            "k8sName": input["k8sName"],
            "password": password,
            "blobPath": blob_path,
        })
        job_name = job_result["jobName"]

        # Poll for job completion
        job_status = "running"
        for _ in range(360):
            check = yield ctx.schedule_activity("wait-for-job", {"jobName": job_name})
            job_status = check["status"]
            if job_status != "running":
                break
            yield ctx.schedule_timer(100)

        yield ctx.schedule_activity("delete-job", {"jobName": job_name})

        if job_status == "succeeded":
            yield ctx.schedule_activity("cms-image-update-state", {
                "imageId": image["id"],
                "state": "ready",
            })
            ctx.trace_info(f"image {input['imageName']} ready")
            return {"status": "ready", "imageId": image["id"], "blobPath": blob_path}
        else:
            yield ctx.schedule_activity("cms-image-update-state", {
                "imageId": image["id"],
                "state": "failed",
            })
            return {"status": "failed", "imageId": image["id"]}

    runtime.start()

    try:
        client.start_orchestration(uid("backup-srcdb"), "CreateImage", {
            "k8sName": "pg-srcdb",
            "imageName": "srcdb-snap",
        })
        result = client.wait_for_orchestration(uid("backup-srcdb"), 15_000)

        assert result.status == "Completed"
        assert result.output["status"] == "ready"
        assert result.output["blobPath"].startswith("images/srcdb-snap-")

        img = cms["images"]["img-srcdb-snap"]
        assert img["state"] == "ready"
        assert job_poll_count["value"] >= 3, (
            f"expected ≥3 job polls, got {job_poll_count['value']}"
        )
    finally:
        runtime.shutdown(100)


# ─── 5. System Pruner ────────────────────────────────────────────


def test_system_pruner(provider):
    """Runs periodic pruning with continue-as-new."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))
    prune_count = {"value": 0}

    @runtime.register_activity("system-prune-2")
    def system_prune(ctx, input):
        prune_count["value"] += 1
        ctx.trace_info(f"pruning iteration {input['iteration']} (prune #{prune_count['value']})")
        return {"pruned": prune_count["value"] * 3, "deleted": prune_count["value"]}

    @runtime.register_orchestration("SystemPruner")
    def system_pruner(ctx, input):
        iteration = input.get("iteration", 0)
        ctx.trace_info(f"[v1.0.4] system pruner iteration {iteration}")

        yield ctx.schedule_activity("system-prune-2", {"iteration": iteration})
        yield ctx.schedule_timer(100)

        # Continue as new (eternal orchestration)
        yield ctx.continue_as_new({"iteration": iteration + 1})

    runtime.start()

    try:
        client.start_orchestration(uid("pruner"), "SystemPruner", {"iteration": 0})

        # Let it run a few cycles
        time.sleep(0.8)

        # Cancel to stop
        client.cancel_instance(uid("pruner"), "test-done")

        assert prune_count["value"] >= 2, (
            f"expected ≥2 prune cycles, got {prune_count['value']}"
        )
    finally:
        runtime.shutdown(100)


# ─── 6. Full Lifecycle: Create → Monitor → Delete ────────────────


def test_full_lifecycle(provider):
    """Creates, monitors, then deletes an instance."""
    client = Client(provider)
    runtime = Runtime(provider, RuntimeOptions(dispatcher_poll_interval_ms=50))
    cms = create_cms_state()
    health_check_count = {"value": 0}

    # ── Activities (combined from create + actor + delete) ──

    @runtime.register_activity("cms-create-instance-record")
    def cms_create(ctx, input):
        record = {
            "id": f"inst-{input['name']}",
            "name": input["name"],
            "k8sName": f"pg-{input['name']}",
            "state": "provisioning",
            "dnsName": f"{input['name']}.postgres.local",
        }
        cms["instances"][record["id"]] = record
        return record

    runtime.register_activity("deploy-postgres-v2", lambda ctx, inp: {"statefulSet": inp["k8sName"]})
    runtime.register_activity("wait-for-ready", lambda ctx, inp: {"ready": True})

    @runtime.register_activity("get-connection-strings")
    def get_conn(ctx, input):
        return {
            "host": f"{input['k8sName']}.svc",
            "port": 5432,
            "connectionString": f"postgresql://postgres:s@{input['k8sName']}:5432/postgres",
        }

    runtime.register_activity("test-connection", lambda ctx, inp: {"connected": True, "latencyMs": 2})

    @runtime.register_activity("cms-update-instance-state")
    def update_state(ctx, input):
        inst = cms["instances"].get(input["id"])
        if inst:
            inst["state"] = input["state"]
        return {"ok": True}

    @runtime.register_activity("cms-get-instance-connection")
    def get_inst_conn(ctx, input):
        inst = cms["instances"].get(input["instanceId"])
        if not inst:
            return {"found": False}
        return {"found": True, "connectionString": "pg://test", "state": inst["state"]}

    @runtime.register_activity("cms-record-health-check")
    def record_hc(ctx, input):
        health_check_count["value"] += 1
        cms["health_checks"].append({"instanceId": input["instanceId"], "healthy": input["healthy"]})
        return {"ok": True}

    @runtime.register_activity("cms-get-instance-by-k8s-name")
    def get_by_k8s(ctx, input):
        for inst in cms["instances"].values():
            if inst.get("k8sName") == input["k8sName"]:
                return inst
        return None

    runtime.register_activity("send-external-event", lambda ctx, inp: {"sent": True})
    runtime.register_activity("delete-postgres", lambda ctx, inp: {"deleted": True})

    @runtime.register_activity("cms-delete-instance-record")
    def delete_record(ctx, input):
        cms["instances"].pop(input["id"], None)
        return {"ok": True}

    @runtime.register_activity("cms-free-dns-name")
    def free_dns(ctx, input):
        cms["dns_names"].discard(input["dnsName"])
        return {"ok": True}

    # ── Orchestrations ──

    @runtime.register_orchestration("CreateInstance")
    def create_instance(ctx, input):
        record = yield ctx.schedule_activity("cms-create-instance-record", {"name": input["name"]})
        yield ctx.schedule_activity("deploy-postgres-v2", {"k8sName": record["k8sName"]})
        status = yield ctx.schedule_activity("wait-for-ready", {"k8sName": record["k8sName"]})
        if not status["ready"]:
            return {"status": "failed"}
        conn = yield ctx.schedule_activity("get-connection-strings", {"k8sName": record["k8sName"]})
        yield ctx.schedule_activity("test-connection", {"connectionString": conn["connectionString"]})
        yield ctx.schedule_activity("cms-update-instance-state", {"id": record["id"], "state": "running"})
        return {"status": "created", "instanceId": record["id"]}

    @runtime.register_orchestration("InstanceActor")
    def instance_actor(ctx, input):
        iteration = input.get("iteration", 0)
        conn_info = yield ctx.schedule_activity("cms-get-instance-connection", {
            "instanceId": input["instanceId"],
        })
        if not conn_info["found"] or conn_info["state"] == "deleted":
            return {"status": "exited", "reason": "gone", "iterations": iteration}
        health = yield ctx.schedule_activity("test-connection", {
            "connectionString": conn_info["connectionString"],
        })
        yield ctx.schedule_activity("cms-record-health-check", {
            "instanceId": input["instanceId"],
            "healthy": health["connected"],
            "latencyMs": health["latencyMs"],
        })
        winner = yield ctx.race(
            ctx.schedule_timer(100),
            ctx.wait_for_event("InstanceDeleted"),
        )
        if winner["index"] == 1:
            return {"status": "exited", "reason": "signal", "iterations": iteration + 1}
        yield ctx.continue_as_new({
            "instanceId": input["instanceId"],
            "iteration": iteration + 1,
        })

    @runtime.register_orchestration("DeleteInstance")
    def delete_instance(ctx, input):
        instance = yield ctx.schedule_activity("cms-get-instance-by-k8s-name", {
            "k8sName": input["k8sName"],
        })
        if not instance:
            return {"status": "not_found"}
        yield ctx.schedule_activity("cms-update-instance-state", {"id": instance["id"], "state": "deleting"})
        yield ctx.schedule_activity("delete-postgres", {"k8sName": instance["k8sName"]})
        yield ctx.schedule_activity("cms-free-dns-name", {"dnsName": instance["dnsName"]})
        yield ctx.schedule_activity("cms-delete-instance-record", {"id": instance["id"]})
        return {"status": "deleted"}

    runtime.start()

    try:
        # Phase 1: Create
        client.start_orchestration(uid("create-lifecycle"), "CreateInstance", {"name": "lifecycle"})
        create_result = client.wait_for_orchestration(uid("create-lifecycle"), 10_000)
        assert create_result.output["status"] == "created"

        # Phase 2: Start actor, let it run
        client.start_orchestration(uid("actor-lifecycle"), "InstanceActor", {
            "instanceId": "inst-lifecycle",
            "iteration": 0,
        })
        time.sleep(0.6)
        assert health_check_count["value"] >= 2, (
            f"expected ≥2 health checks, got {health_check_count['value']}"
        )

        # Phase 3: Mark instance deleted so actor exits
        cms["instances"]["inst-lifecycle"]["state"] = "deleted"
        actor_result = client.wait_for_orchestration(uid("actor-lifecycle"), 10_000)
        assert actor_result.output["status"] == "exited"

        # Reset state for delete orchestration
        cms["instances"]["inst-lifecycle"] = {
            "id": "inst-lifecycle",
            "name": "lifecycle",
            "k8sName": "pg-lifecycle",
            "state": "running",
            "dnsName": "lifecycle.postgres.local",
        }
        cms["dns_names"].add("lifecycle.postgres.local")

        # Phase 4: Delete
        client.start_orchestration(uid("delete-lifecycle"), "DeleteInstance", {
            "k8sName": "pg-lifecycle",
        })
        delete_result = client.wait_for_orchestration(uid("delete-lifecycle"), 10_000)
        assert delete_result.output["status"] == "deleted"

        # Verify clean state
        assert len(cms["instances"]) == 0
    finally:
        runtime.shutdown(100)
