from apscheduler.schedulers.blocking import BlockingScheduler

from app.config import POLL_INTERVAL_SECONDS
from app.ingest import VehiclePositions
from app.logging_config import configure_logging


def main() -> None:
    configure_logging()

    vehicle_positions = VehiclePositions()

    scheduler = BlockingScheduler()
    scheduler.add_job(
        vehicle_positions.run,
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
