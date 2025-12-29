from app.pipelines.vehicle_positions import VehiclePositionsPipeline
from app.logging_config import configure_logging


def main():
    configure_logging()
    VehiclePositionsPipeline().run_forever()


if __name__ == "__main__":
    main()
