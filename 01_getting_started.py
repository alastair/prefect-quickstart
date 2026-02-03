from prefect import flow, task
import random

from prefect.futures import as_completed
from prefect.logging import get_run_logger


@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(5000), k=300)]

@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    logger = get_run_logger()
    logger.info(f"Processing customer {customer_id}")
    return f"Processed {customer_id}"

@flow
def main() -> list[str]:
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
