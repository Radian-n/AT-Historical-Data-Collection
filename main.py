from apscheduler.schedulers.blocking import BlockingScheduler

from app.config import POLL_INTERVAL_SECONDS
from app.logging_config import configure_logging
from app.pipelines.base import RealtimePipeline
from app.schemas.vehicle_positions import VehiclePositionSchema


def main() -> None:
    configure_logging()

    vehicle_positions = RealtimePipeline(
        url="https://api.at.govt.nz/realtime/legacy/vehiclelocations",
        table_name="vehicle_positions",
        table_schema=VehiclePositionSchema,
    )

    scheduler = BlockingScheduler()
    scheduler.add_job(
        vehicle_positions.run_once,
        "interval",
        seconds=POLL_INTERVAL_SECONDS,
        misfire_grace_time=POLL_INTERVAL_SECONDS,  # Skip if more than 1 interval late
    )

    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
