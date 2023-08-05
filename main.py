#!/usr/bin/env python3
"""Monitor cron jobs, track execution history, and alert on failures."""

import sqlite3
import subprocess
import smtplib
import argparse
import json
import time
import sys
from datetime import datetime
from email.mime.text import MIMEText
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "cron_monitor.db"
CONFIG_PATH = Path(__file__).parent / "config.json"


@contextmanager
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                command TEXT NOT NULL,
                schedule TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                duration_seconds REAL,
                status TEXT DEFAULT 'running',
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            );
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                execution_id INTEGER,
                alert_type TEXT NOT NULL,
                message TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            );
        """)


def load_config() -> dict:
    default = {
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "smtp_user": "",
        "smtp_password": "",
        "alert_email": "",
        "slack_webhook": "",
        "max_retries": 3,
        "alert_on_failure": True,
        "alert_on_duration_exceed": 300,
    }
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            user_config = json.load(f)
            default.update(user_config)
    return default


def add_job(name: str, command: str, schedule: str):
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT INTO jobs (name, command, schedule, created_at) VALUES (?, ?, ?, ?)",
                (name, command, schedule, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            print(f"  Job '{name}' added successfully.")
        except sqlite3.IntegrityError:
            print(f"  Job '{name}' already exists.")


def list_jobs():
    with get_db() as conn:
        jobs = conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
    if not jobs:
        print("  No jobs registered.")
        return
    print(f"\n{'ID':>4} | {'Name':<20} | {'Schedule':<15} | {'Enabled':<8} | Command")
    print("-" * 90)
    for job in jobs:
        enabled = "Yes" if job["enabled"] else "No"
        print(f"{job['id']:>4} | {job['name']:<20} | {job['schedule']:<15} | {enabled:<8} | {job['command']}")


def run_job(name: str, config: dict):
    with get_db() as conn:
        job = conn.execute("SELECT * FROM jobs WHERE name = ?", (name,)).fetchone()
    if not job:
        print(f"  Job '{name}' not found.")
        return

    print(f"  Running job: {name}")
    print(f"  Command: {job['command']}")

    start_time = datetime.now()
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO executions (job_id, started_at, status) VALUES (?, ?, 'running')",
            (job["id"], start_time.strftime("%Y-%m-%d %H:%M:%S")),
        )
        exec_id = cursor.lastrowid

    try:
        result = subprocess.run(
            job["command"],
            shell=True,
            capture_output=True,
            text=True,
            timeout=config.get("alert_on_duration_exceed", 300),
        )
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        status = "success" if result.returncode == 0 else "failed"

        with get_db() as conn:
            conn.execute(
                """UPDATE executions SET finished_at=?, exit_code=?, stdout=?, stderr=?,
                   duration_seconds=?, status=? WHERE id=?""",
                (
                    end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    result.returncode,
                    result.stdout[:5000],
                    result.stderr[:5000],
                    duration,
                    status,
                    exec_id,
                ),
            )

        print(f"  Status: {status} | Exit code: {result.returncode} | Duration: {duration:.2f}s")
        if result.stdout.strip():
            print(f"  Stdout: {result.stdout.strip()[:200]}")
        if result.stderr.strip():
            print(f"  Stderr: {result.stderr.strip()[:200]}")

        if status == "failed" and config.get("alert_on_failure"):
            send_alert(job, exec_id, f"Job '{name}' failed with exit code {result.returncode}", config)

    except subprocess.TimeoutExpired:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        with get_db() as conn:
            conn.execute(
                "UPDATE executions SET finished_at=?, duration_seconds=?, status='timeout' WHERE id=?",
                (end_time.strftime("%Y-%m-%d %H:%M:%S"), duration, exec_id),
            )
        print(f"  TIMEOUT after {duration:.2f}s")
        send_alert(job, exec_id, f"Job '{name}' timed out after {duration:.0f}s", config)


def send_alert(job: dict, exec_id: int, message: str, config: dict):
    print(f"  ALERT: {message}")

    with get_db() as conn:
        conn.execute(
            "INSERT INTO alerts (job_id, execution_id, alert_type, message, sent_at) VALUES (?, ?, ?, ?, ?)",
            (job["id"], exec_id, "failure", message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )

    if config.get("smtp_user") and config.get("alert_email"):
        try:
            msg = MIMEText(f"Cron Job Monitor Alert\n\n{message}\n\nJob: {job['name']}\nCommand: {job['command']}")
            msg["Subject"] = f"[ALERT] Cron Job Failed: {job['name']}"
            msg["From"] = config["smtp_user"]
            msg["To"] = config["alert_email"]

            with smtplib.SMTP(config["smtp_host"], config["smtp_port"]) as server:
                server.starttls()
                server.login(config["smtp_user"], config["smtp_password"])
                server.send_message(msg)
            print("  Email alert sent.")
        except Exception as e:
            print(f"  Failed to send email: {e}")


def show_history(name: str | None = None, limit: int = 20):
    with get_db() as conn:
        if name:
            job = conn.execute("SELECT id FROM jobs WHERE name = ?", (name,)).fetchone()
            if not job:
                print(f"  Job '{name}' not found.")
                return
            rows = conn.execute(
                """SELECT e.*, j.name as job_name FROM executions e
                   JOIN jobs j ON e.job_id = j.id WHERE j.name = ?
                   ORDER BY e.id DESC LIMIT ?""",
                (name, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT e.*, j.name as job_name FROM executions e
                   JOIN jobs j ON e.job_id = j.id ORDER BY e.id DESC LIMIT ?""",
                (limit,),
            ).fetchall()

    if not rows:
        print("  No execution history.")
        return

    print(f"\n{'ID':>5} | {'Job':<20} | {'Status':<10} | {'Exit':>4} | {'Duration':>10} | Started At")
    print("-" * 90)
    for row in rows:
        duration = f"{row['duration_seconds']:.2f}s" if row["duration_seconds"] else "N/A"
        status_icon = {"success": "✓", "failed": "✗", "timeout": "⏱", "running": "⟳"}.get(row["status"], "?")
        print(f"{row['id']:>5} | {row['job_name']:<20} | {status_icon} {row['status']:<8} | {row['exit_code'] or 'N/A':>4} | {duration:>10} | {row['started_at']}")


def show_stats():
    with get_db() as conn:
        jobs = conn.execute("SELECT * FROM jobs").fetchall()

    if not jobs:
        print("  No jobs registered.")
        return

    print(f"\n{'Job':<20} | {'Total':>6} | {'Success':>8} | {'Failed':>7} | {'Avg Duration':>13} | {'Success Rate':>12}")
    print("-" * 85)

    for job in jobs:
        with get_db() as conn:
            stats = conn.execute(
                """SELECT COUNT(*) as total,
                   SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as success,
                   SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
                   AVG(duration_seconds) as avg_duration
                   FROM executions WHERE job_id = ?""",
                (job["id"],),
            ).fetchone()

        total = stats["total"] or 0
        success = stats["success"] or 0
        failed = stats["failed"] or 0
        avg_dur = f"{stats['avg_duration']:.2f}s" if stats["avg_duration"] else "N/A"
        rate = f"{success/total*100:.1f}%" if total > 0 else "N/A"
        print(f"{job['name']:<20} | {total:>6} | {success:>8} | {failed:>7} | {avg_dur:>13} | {rate:>12}")


def main():
    parser = argparse.ArgumentParser(description="Cron Job Monitor")
    subparsers = parser.add_subparsers(dest="command")

    add_p = subparsers.add_parser("add", help="Register a new job")
    add_p.add_argument("name", help="Job name")
    add_p.add_argument("cmd", help="Command to execute")
    add_p.add_argument("--schedule", default="*/5 * * * *", help="Cron schedule expression")

    subparsers.add_parser("list", help="List all registered jobs")

    run_p = subparsers.add_parser("run", help="Run a job manually")
    run_p.add_argument("name", help="Job name")

    hist_p = subparsers.add_parser("history", help="Show execution history")
    hist_p.add_argument("--name", help="Filter by job name")
    hist_p.add_argument("--limit", type=int, default=20, help="Number of entries")

    subparsers.add_parser("stats", help="Show job statistics")

    args = parser.parse_args()
    init_db()
    config = load_config()

    if args.command == "add":
        add_job(args.name, args.cmd, args.schedule)
    elif args.command == "list":
        list_jobs()
    elif args.command == "run":
        run_job(args.name, config)
    elif args.command == "history":
        show_history(args.name, args.limit)
    elif args.command == "stats":
        show_stats()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
