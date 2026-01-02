from apscheduler.schedulers.blocking import BlockingScheduler

from app.config import POLL_INTERVAL_SECONDS
from app.ingest import VehiclePositions, TripUpdates
from app.logging_config import configure_logging


def main() -> None:
    configure_logging()

    vehicle_positions = VehiclePositions()
    trip_updates = TripUpdates()

    scheduler = BlockingScheduler()
    scheduler.add_job(
        vehicle_positions.run,
        "interval",
        seconds=POLL_INTERVAL_SECONDS,
        misfire_grace_time=POLL_INTERVAL_SECONDS,
    )
    scheduler.add_job(
        trip_updates.run,
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
