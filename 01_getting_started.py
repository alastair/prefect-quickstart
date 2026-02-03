from prefect import flow, task
import random

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
    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs
    results = process_customer.map(customer_ids)
    return results


if __name__ == "__main__":
    main()
