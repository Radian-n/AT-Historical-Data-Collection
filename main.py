from typing import Never

from app.logging_config import configure_logging
from app.pipelines.vehicle_positions import VehiclePositionsPipeline


def main() -> Never:
    configure_logging()
    VehiclePositionsPipeline().run_forever()


if __name__ == "__main__":
    main()
