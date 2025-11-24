"""
Control Manager
----------------
Đóng gói các thao tác với Control DB (job, step, log, config)
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional, Any
from datetime import datetime

try:
    from .control_connect import (  # type: ignore
        get_control_connection,
        ensure_control_tables,
        create_etl_job,
        update_etl_job,
        start_etl_step,
        update_etl_step,
        log_job_event,
        log_file_audit,
        get_source_config as get_source_config_from_db,
    )
except ImportError:
    import sys

    CONTROL_DIR = os.path.dirname(os.path.abspath(__file__))
    if CONTROL_DIR not in sys.path:
        sys.path.insert(0, CONTROL_DIR)
    from control_connect import (  # type: ignore
        get_control_connection,
        ensure_control_tables,
        create_etl_job,
        update_etl_job,
        start_etl_step,
        update_etl_step,
        log_job_event,
        log_file_audit,
        get_source_config as get_source_config_from_db,
    )


class ControlManager:
    """Context manager tiện dụng cho scripts ETL"""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.conn = None
        self.job_id: Optional[int] = None
        self.batch_id: Optional[int] = None
        self._step_ids: Dict[str, int] = {}
        self._job_closed = False
        self._records_processed = 0
        self._error_message: Optional[str] = None

    def __enter__(self) -> "ControlManager":
        self.conn = get_control_connection()
        ensure_control_tables(self.conn)
        self.job_id = create_etl_job(self.job_name, conn=self.conn)
        log_job_event(self.job_id, "job_start", f"Job '{self.job_name}' bắt đầu", conn=self.conn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.job_id is None or self.conn is None:
            return
        if exc_type:
            error_msg = str(exc_val)
            self._error_message = error_msg
            log_job_event(
                self.job_id,
                "job_exception",
                "Job kết thúc với exception",
                {"error": error_msg},
                conn=self.conn,
            )
            update_etl_job(self.job_id, "failed", self._records_processed, error_msg, conn=self.conn)
        elif not self._job_closed:
            update_etl_job(self.job_id, "completed", self._records_processed, None, conn=self.conn)
        if self.conn:
            self.conn.close()

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------
    def get_source_config(self, source_name: str, required_keys: Optional[List[str]] = None) -> Dict[str, str]:
        if self.conn is None:
            raise RuntimeError("ControlManager chưa được khởi tạo đúng cách")
        config = get_source_config_from_db(source_name, required_keys=required_keys, conn=self.conn)
        log_job_event(
            self.job_id,
            "config_loaded",
            f"Đã load config cho source {source_name}",
            {"keys": list(config.keys())},
            conn=self.conn,
        )
        return config

    # ------------------------------------------------------------------
    # Step helpers
    # ------------------------------------------------------------------
    def start_step(self, step_name: str) -> int:
        if self.job_id is None or self.conn is None:
            raise RuntimeError("ControlManager chưa được khởi tạo")
        step_id = start_etl_step(self.job_id, step_name, conn=self.conn)
        self._step_ids[step_name] = step_id
        return step_id

    def finish_step(
        self,
        step_name: str,
        status: str = "completed",
        records_processed: int = 0,
        info: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        if self.conn is None:
            raise RuntimeError("ControlManager chưa được khởi tạo")
        step_id = self._step_ids.get(step_name)
        if step_id is None:
            raise KeyError(f"Step '{step_name}' chưa được start")
        update_etl_step(
            step_id,
            status=status,
            records_processed=records_processed,
            info=info,
            error_message=error_message,
            conn=self.conn,
        )
        if status == "completed":
            self._records_processed += records_processed

    # ------------------------------------------------------------------
    # Job level helpers
    # ------------------------------------------------------------------
    def mark_job_completed(self, records_processed: int = 0, message: Optional[str] = None):
        if self.job_id is None or self.conn is None:
            raise RuntimeError("ControlManager chưa được khởi tạo")
        self._records_processed = records_processed
        log_job_event(
            self.job_id,
            "job_completed",
            message or "Job hoàn thành",
            {"records_processed": records_processed},
            conn=self.conn,
        )
        update_etl_job(self.job_id, "completed", records_processed, None, conn=self.conn)
        self._job_closed = True

    def mark_job_failed(self, error_message: str):
        if self.job_id is None or self.conn is None:
            raise RuntimeError("ControlManager chưa được khởi tạo")
        self._error_message = error_message
        log_job_event(
            self.job_id,
            "job_failed",
            "Job thất bại",
            {"error": error_message},
            conn=self.conn,
        )
        update_etl_job(self.job_id, "failed", self._records_processed, error_message, conn=self.conn)
        self._job_closed = True

    def log_event(self, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None):
        if self.job_id is None or self.conn is None:
            raise RuntimeError("ControlManager chưa được khởi tạo")
        log_job_event(self.job_id, event_type, message, payload, conn=self.conn)

    def log_file(
        self,
        file_path: str,
        records_count: int,
        status: str,
        error_message: Optional[str] = None,
    ):
        if self.conn is None:
            raise RuntimeError("ControlManager chưa được khởi tạo")
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else None
        log_file_audit(
            file_name=file_name,
            file_path=file_path,
            file_size=file_size or 0,
            records_count=records_count,
            status=status,
            batch_id=self.batch_id,
            error_message=error_message,
            conn=self.conn,
        )


