import pytest

import main
from livy_client import LivyClient


def test_build_uri_azure():
    uri = main.build_uri("dataplatform", "spark/spark-bridge/jobs/pi.py")
    assert uri == "abfss://spark@dataplatform.dfs.core.windows.net/spark-bridge/jobs/pi.py"


def test_build_uri_local():
    assert main.build_uri("local", "jobs/pi.py") == "file:///data/shared/jobs/pi.py"


def test_build_uri_requires_container_and_file():
    with pytest.raises(SystemExit):
        main.build_uri("dataplatform", "onlycontainer")


def test_submit_batch_payload(monkeypatch):
    client = LivyClient("http://localhost:8998")
    sent = {}

    def fake_post(path, payload):
        sent["path"], sent["payload"] = path, payload
        return {"id": 7}

    monkeypatch.setattr(client, "_post", fake_post)
    job = client.submit_batch(
        file="abfss://c@a.dfs.core.windows.net/x.py",
        name="X",
        num_executors=2,
        executor_cores=1,
        executor_memory="1g",
        driver_memory="512m",
    )
    assert job.batch_id == 7
    assert sent["path"] == "/batches"
    assert sent["payload"] == {
        "file": "abfss://c@a.dfs.core.windows.net/x.py",
        "name": "X",
        "numExecutors": 2,
        "executorCores": 1,
        "executorMemory": "1g",
        "driverMemory": "512m",
    }


def test_submit_batch_omits_unset_options(monkeypatch):
    client = LivyClient("http://localhost:8998")
    sent = {}
    monkeypatch.setattr(client, "_post", lambda p, body: sent.update(body) or {"id": 1})
    client.submit_batch(file="file:///x.py")
    assert sent == {"file": "file:///x.py"}
