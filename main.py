from apscheduler.schedulers.blocking import BlockingScheduler

from app.config import POLL_INTERVAL_SECONDS
from app.entities.vehicle_positions import VehiclePositionEntity
from app.logging_config import configure_logging
from app.pipeline import RealtimePipeline


def main() -> None:
    configure_logging()

    vehicle_positions = RealtimePipeline(VehiclePositionEntity)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        vehicle_positions.run_once,
        "interval",
        seconds=POLL_INTERVAL_SECONDS,
        id=VehiclePositionEntity.TABLE_NAME,
        misfire_grace_time=POLL_INTERVAL_SECONDS,
    )

    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
