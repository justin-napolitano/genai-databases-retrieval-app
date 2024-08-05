import asyncio
import logging
import datastore
from app import parse_config

logging.basicConfig(level=logging.DEBUG)

async def main() -> None:
    airports_ds_path = "../data/airport_dataset.csv"
    amenities_ds_path = "../data/amenity_dataset.csv"
    flights_ds_path = "../data/flights_dataset.csv"
    policies_ds_path = "../data/cymbalair_policy.csv"

    cfg = parse_config("config.yml")
    ds = await datastore.create(cfg.datastore)

    try:
        logging.info("Loading datasets...")
        airports, amenities, flights, policies = await ds.load_dataset(
            airports_ds_path, amenities_ds_path, flights_ds_path, policies_ds_path
        )
        logging.info("Datasets loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading datasets: {e}")
        return

    try:
        logging.info("Initializing data...")
        await ds.initialize_data(airports, amenities, flights, policies)
        logging.info("Data initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing data: {e}")
    finally:
        await ds.close()
        logging.info("Database connection closed.")

    print("Database init done.")

if __name__ == "__main__":
    asyncio.run(main())
