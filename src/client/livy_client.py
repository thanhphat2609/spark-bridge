"""
livy_client.py
--------------
Reusable Apache Livy REST API client.

Design goal: a client machine that has only Python and the 'requests' library
installed can submit and monitor PySpark jobs on a remote Spark cluster, with
zero local Spark or Java dependency.

Usage (batch):
    from livy_client import LivyClient

    client = LivyClient("http://<livy-host>:8998")
    job = client.submit_batch(
        file="local:///opt/spark-jobs/pi_estimation.py",
        args=["1000000"],
        name="PiEstimation",
    )
    final_state = job.wait()

Usage (interactive session):
    with client.create_session(kind="pyspark") as session:
        output = session.run("spark.version")
        print(output)
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Batch job handle
# ---------------------------------------------------------------------------

class LivyBatchJob:
    """Handle for a Livy batch job submitted via POST /batches."""

    def __init__(self, batch_id: int, client: "LivyClient") -> None:
        self.batch_id = batch_id
        self._client = client

    # ------------------------------------------------------------------
    def get_state(self) -> str:
        """Return the current state string reported by Livy."""
        response = self._client._get(f"/batches/{self.batch_id}")
        return response.get("state", "unknown")

    def get_log(self) -> List[str]:
        """Return the accumulated log lines for this batch job."""
        response = self._client._get(f"/batches/{self.batch_id}/log")
        return response.get("log", [])

    def wait(self, poll_interval: int = 5, timeout: int = 3600) -> str:
        """
        Block until the job reaches a terminal state.

        Args:
            poll_interval: Seconds between state polls.
            timeout:       Maximum seconds to wait before raising TimeoutError.

        Returns:
            Final state string: 'success' or 'dead'.
        """
        terminal_states = {"success", "dead", "killed"}
        elapsed = 0

        while elapsed < timeout:
            state = self.get_state()
            logger.info("Batch %d state: %s", self.batch_id, state)
            if state in terminal_states:
                return state
            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(
            f"Batch job {self.batch_id} did not complete within {timeout} seconds."
        )

    def __repr__(self) -> str:
        return f"LivyBatchJob(id={self.batch_id})"


# ---------------------------------------------------------------------------
# Interactive session handle
# ---------------------------------------------------------------------------

class LivySession:
    """
    Handle for an interactive Livy session (POST /sessions).

    Allows sending arbitrary PySpark code as plain strings without any local
    Spark installation.  The session context manager closes the session on exit
    to release cluster resources.
    """

    def __init__(self, session_id: int, client: "LivyClient") -> None:
        self.session_id = session_id
        self._client = client

    # ------------------------------------------------------------------
    def get_state(self) -> str:
        response = self._client._get(f"/sessions/{self.session_id}")
        return response.get("state", "unknown")

    def wait_until_ready(self, poll_interval: int = 5, timeout: int = 300) -> None:
        """Block until the session enters 'idle' (ready to execute code)."""
        terminal_error_states = {"dead", "killed", "error"}
        elapsed = 0

        while elapsed < timeout:
            state = self.get_state()
            if state == "idle":
                return
            if state in terminal_error_states:
                raise RuntimeError(
                    f"Session {self.session_id} entered state '{state}' during startup."
                )
            logger.info("Session %d is '%s', waiting ...", self.session_id, state)
            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(
            f"Session {self.session_id} did not become ready within {timeout} seconds."
        )

    def run(self, code: str, timeout: int = 300) -> Dict[str, Any]:
        """
        Execute a PySpark code snippet and return the statement output dict.

        The session must be in 'idle' state before executing.  This method
        handles the wait automatically.

        Args:
            code:    PySpark source code to run.
            timeout: Seconds to wait for the statement to complete.

        Returns:
            Livy statement output dict, e.g.:
            {"status": "ok", "data": {"text/plain": "..."}}
        """
        self.wait_until_ready()

        payload = {"code": code}
        response = self._client._post(
            f"/sessions/{self.session_id}/statements", payload
        )
        stmt_id = response["id"]

        elapsed = 0
        terminal_states = {"available", "error", "cancelled"}

        while elapsed < timeout:
            stmt = self._client._get(
                f"/sessions/{self.session_id}/statements/{stmt_id}"
            )
            if stmt["state"] in terminal_states:
                return stmt.get("output", {})
            time.sleep(3)
            elapsed += 3

        raise TimeoutError(
            f"Statement {stmt_id} did not complete within {timeout} seconds."
        )

    def close(self) -> None:
        """Delete the session and free cluster resources."""
        self._client._delete(f"/sessions/{self.session_id}")
        logger.info("Session %d closed.", self.session_id)

    # Context manager support
    def __enter__(self) -> "LivySession":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"LivySession(id={self.session_id})"


# ---------------------------------------------------------------------------
# Main client
# ---------------------------------------------------------------------------

class LivyClient:
    """
    Apache Livy REST API client.

    Enables remote PySpark job submission from any Python environment.
    The only dependency is the 'requests' library -- no Spark, no Java.

    Args:
        host:    Full base URL of the Livy server, e.g. 'http://10.0.0.5:8998'.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(self, host: str, timeout: int = 30) -> None:
        self.host = host.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    # ------------------------------------------------------------------
    # Low-level HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Dict[str, Any]:
        url = f"{self.host}{path}"
        response = self._session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.host}{path}"
        response = self._session.post(
            url, data=json.dumps(payload), timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def _delete(self, path: str) -> None:
        url = f"{self.host}{path}"
        response = self._session.delete(url, timeout=self.timeout)
        response.raise_for_status()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def health_check(self) -> bool:
        """Return True if the Livy server is reachable, False otherwise."""
        try:
            self._get("/sessions")
            return True
        except (requests.ConnectionError, requests.HTTPError, requests.Timeout):
            return False

    def submit_batch(
        self,
        file: str,
        args: Optional[List[str]] = None,
        conf: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        num_executors: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        driver_memory: Optional[str] = None,
    ) -> LivyBatchJob:
        """
        Submit a PySpark script as a batch job via POST /batches.

        Args:
            file:             Path to the Python script accessible by the Livy
                              server, e.g. 'local:///opt/spark-jobs/pi.py'.
            args:             Command-line arguments forwarded to the script.
            conf:             Spark configuration overrides.
            name:             Application name shown in the Spark UI.
            num_executors:    Number of Spark executors to request.
            executor_cores:   vCPU cores per executor.
            executor_memory:  Executor JVM heap, e.g. '2g'.
            driver_memory:    Driver JVM heap, e.g. '1g'.

        Returns:
            LivyBatchJob instance for polling and log retrieval.
        """
        payload: Dict[str, Any] = {"file": file}

        if args:
            payload["args"] = args
        if conf:
            payload["conf"] = conf
        if name:
            payload["name"] = name
        if num_executors is not None:
            payload["numExecutors"] = num_executors
        if executor_cores is not None:
            payload["executorCores"] = executor_cores
        if executor_memory:
            payload["executorMemory"] = executor_memory
        if driver_memory:
            payload["driverMemory"] = driver_memory

        response = self._post("/batches", payload)
        batch_id = response["id"]
        logger.info("Batch job submitted. Livy ID: %d", batch_id)
        return LivyBatchJob(batch_id, self)

    def create_session(
        self,
        kind: str = "pyspark",
        name: Optional[str] = None,
        conf: Optional[Dict[str, str]] = None,
    ) -> LivySession:
        """
        Create an interactive Livy session via POST /sessions.

        Args:
            kind:  Interpreter kind: 'pyspark', 'spark' (Scala), or 'sparkr'.
            name:  Optional session label shown in the Livy UI.
            conf:  Spark configuration overrides applied to this session.

        Returns:
            LivySession instance (supports context-manager usage).
        """
        payload: Dict[str, Any] = {"kind": kind}

        if name:
            payload["name"] = name
        if conf:
            payload["conf"] = conf

        response = self._post("/sessions", payload)
        session_id = response["id"]
        logger.info("Interactive session created. Livy ID: %d", session_id)
        return LivySession(session_id, self)

    def list_batches(self) -> List[Dict[str, Any]]:
        """Return all active and recently completed batch jobs."""
        return self._get("/batches").get("batches", [])

    def list_sessions(self) -> List[Dict[str, Any]]:
        """Return all active interactive sessions."""
        return self._get("/sessions").get("sessions", [])
