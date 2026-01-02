from apscheduler.schedulers.blocking import BlockingScheduler

from app.cleanup import cleanup_all
from app.config import CLEANUP_MINUTE, POLL_INTERVAL_SECONDS
from app.ingest import combined_ingest
from app.logging_config import configure_logging


def main() -> None:
    configure_logging()

    scheduler = BlockingScheduler()
    scheduler.add_job(
        combined_ingest,
        "interval",
        seconds=POLL_INTERVAL_SECONDS,
        misfire_grace_time=POLL_INTERVAL_SECONDS,
    )

    # Hourly cleanup (dedupe + compact previous hour)
    scheduler.add_job(
        cleanup_all,
        "cron",
        minute=CLEANUP_MINUTE,
    )

    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
