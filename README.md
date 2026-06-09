# Cron Job Monitor

Track cron job executions, monitor success/failure rates, and alert on failures via email.

## Features

- Register and manage cron jobs
- Run jobs manually with full output capture
- Execution history with status, exit codes, and duration
- Job statistics (success rate, average duration)
- Email alerts on job failures
- Timeout detection
- SQLite-backed persistent storage

## Usage

```bash
# Register a job
python main.py add "backup-db" "pg_dump mydb > backup.sql" --schedule "0 2 * * *"
python main.py add "health-check" "curl -s http://localhost:8080/health"
python main.py add "disk-check" "df -h | head -5"

# List all jobs
python main.py list

# Run a job
python main.py run backup-db

# View execution history
python main.py history
python main.py history --name backup-db --limit 10

# View statistics
python main.py stats
```

## Configuration

Edit `config.json` to set up email alerts (SMTP settings).

<sub><sup>Originally developed and tested locally during learning. Later organized and pushed to GitHub for portfolio visibility.</sup></sub>
