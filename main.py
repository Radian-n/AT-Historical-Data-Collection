from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from app.compaction import compact_all
from app.config import (
    COMPACTION_MINUTE,
    POLL_INTERVAL_SECONDS,
    PROCESSING_HOUR_NZT,
    STATIC_INGEST_HOUR_NZT,
)
from app.logging_config import configure_logging
from app.processing import process_all
from app.realtime_ingest import combined_ingest
from app.static_ingest import static_ingest


def main() -> None:
    configure_logging()

    scheduler = BlockingScheduler()
    scheduler.add_job(
        combined_ingest,
        "interval",
        seconds=POLL_INTERVAL_SECONDS,
        misfire_grace_time=POLL_INTERVAL_SECONDS,
        next_run_time=datetime.now(),
    )

    # Hourly compaction (Delta OPTIMIZE + retention cleanup)
    # Reduces file count and removes old raw data
    scheduler.add_job(
        compact_all,
        "cron",
        minute=COMPACTION_MINUTE,
        next_run_time=datetime.now(),
    )

    # Daily processing (raw -> processed transformation)
    # Runs at PROCESSING_HOUR_NZT (NZ time) to process previous day's data
    scheduler.add_job(
        process_all,
        "cron",
        hour=PROCESSING_HOUR_NZT,
        minute=0,
        timezone="Pacific/Auckland",
        next_run_time=datetime.now(),
    )

    # Daily GTFS static data check
    scheduler.add_job(
        static_ingest,
        "cron",
        hour=STATIC_INGEST_HOUR_NZT,
        minute=0,
        timezone="Pacific/Auckland",
        next_run_time=datetime.now(),
    )

    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
