from typing import Never

from app.logging_config import configure_logging
from app.pipelines.base import RealtimePipeline
from app.schemas.vehicle_positions import VehiclePositionSchema


def main() -> Never:
    configure_logging()

    pipeline = RealtimePipeline(
        url="https://api.at.govt.nz/realtime/legacy/vehiclelocations",
        table_name="vehicle_positions",
        table_schema=VehiclePositionSchema,
    )
    pipeline.run_forever()


if __name__ == "__main__":
    main()
