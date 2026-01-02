from apscheduler.schedulers.blocking import BlockingScheduler

from app.config import POLL_INTERVAL_SECONDS
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

    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
