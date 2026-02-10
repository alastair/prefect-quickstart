from prefect import flow, task
import random
import time
import sentry_sdk

from prefect.futures import as_completed
from prefect.logging import get_run_logger


def setup_sentry():
    logger = get_run_logger()
    logger.info("Setting up Sentry")
    sentry_sdk.init(
        dsn="https://983ff9aae6b147ce65161da073d05404@logserver.mtg.upf.edu/23",
        # Add data like request headers and IP for users,
        # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
        send_default_pii=True,
        traces_sample_rate=1.0,
        enable_logs=True,
    )

@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(5000), k=300)]

@task(tags=["process-customer"])
def process_customer(customer_id: str) -> str:
    # Process a single customer
    logger = get_run_logger()
    logger.info(f"Processing customer {customer_id}")
    time.sleep(random.randint(1, 50)/ 10)
    return f"Processed {customer_id}"

@flow
def main() -> list[str]:
    setup_sentry()
    logger = get_run_logger()
    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs\
    futures = []
    for customer_id in customer_ids:
        futures.append(process_customer.submit(customer_id))

    results = []
    for future in as_completed(futures):
        results.append(future.result())

    logger.info(f"Processed {len(results)} customers")
    return results


if __name__ == "__main__":
    main()
