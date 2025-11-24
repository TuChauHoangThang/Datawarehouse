-- ================================
-- CONTROL DATABASE SCHEMA
-- ================================

-- Tạo database control nếu chưa có (cần chạy thủ công hoặc qua init script)
-- CREATE DATABASE control_dw;

-- Bảng điều khiển ETL job
CREATE TABLE IF NOT EXISTS etl_job_control (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- 'running', 'completed', 'failed'
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng audit log cho file
CREATE TABLE IF NOT EXISTS file_audit_log (
    log_id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_path TEXT NOT NULL,
    file_size BIGINT,
    records_count INT,
    status VARCHAR(20) NOT NULL, -- 'loaded', 'failed', 'skipped'
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id INT,
    error_message TEXT
);

-- Bảng lịch sử batch run
CREATE TABLE IF NOT EXISTS batch_run_history (
    batch_id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    batch_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL, -- 'success', 'failed', 'partial'
    files_processed INT DEFAULT 0,
    total_records INT DEFAULT 0,
    error_summary TEXT
);

-- Tạo index để tăng tốc độ truy vấn
CREATE INDEX IF NOT EXISTS idx_file_audit_batch ON file_audit_log(batch_id);
CREATE INDEX IF NOT EXISTS idx_file_audit_status ON file_audit_log(status);
CREATE INDEX IF NOT EXISTS idx_etl_job_status ON etl_job_control(status);
CREATE INDEX IF NOT EXISTS idx_batch_date ON batch_run_history(batch_date);

-- Bảng log chi tiết từng bước ETL
CREATE TABLE IF NOT EXISTS etl_step_log (
    step_id SERIAL PRIMARY KEY,
    job_id INT NOT NULL REFERENCES etl_job_control(job_id) ON DELETE CASCADE,
    step_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INT DEFAULT 0,
    info TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_step_job ON etl_step_log(job_id);
CREATE INDEX IF NOT EXISTS idx_step_status ON etl_step_log(status);

-- Bảng log sự kiện của job (ghi lại config, stdout/stderr, ...)
CREATE TABLE IF NOT EXISTS etl_job_event (
    event_id SERIAL PRIMARY KEY,
    job_id INT NOT NULL REFERENCES etl_job_control(job_id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    message TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_event_job ON etl_job_event(job_id);

-- Bảng cấu hình nguồn dữ liệu ngoài (API, file, v.v.)
CREATE TABLE IF NOT EXISTS external_source_config (
    config_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    config_key VARCHAR(100) NOT NULL,
    config_value TEXT NOT NULL,
    is_secret BOOLEAN DEFAULT FALSE,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_name, config_key)
);

CREATE INDEX IF NOT EXISTS idx_external_source ON external_source_config(source_name);

