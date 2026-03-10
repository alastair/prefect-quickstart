from prefect import flow, task
import time
import sentry_sdk
import logging

from prefect.client.schemas import FlowRun
from prefect import get_client
from prefect.deployments import run_deployment
from prefect.logging import get_run_logger
from prefect.runtime import get_run_context
from helper import wait_for_flows_to_finish

def setup_sentry():
    httpx_logger = logging.getLogger("httpx")
    httpx_logger.setLevel(logging.INFO)
    logger = get_run_logger()
    logger.info("Setting up Sentry")
    sentry_sdk.init(
        dsn="https://5b6b9638894cd3d9ec0404245dcacb74@o1072865.ingest.us.sentry.io/4510860673810432",
        send_default_pii=True,
        traces_sample_rate=1.0,
        enable_logs=True,
    )


def spin_some_api_calls():
    context = get_run_context()
    flow_run_id = context.flow_run.id
    with get_client(sync_client=True) as client:
        for i in range(20):
            client.read_flow_run(flow_run_id)

@task
def subflow_task(argument: str):
    logger = get_run_logger()
    logger.info(f"Subflow task {argument} (starting)")
    spin_some_api_calls()
    time.sleep(20)
    if argument == "6":
        logger.info("Special case subflow 6, going to crash")
        raise ValueError("Special case subflow 6")
    logger.info(f"Subflow task {argument} (finished)")


@flow(name="subflow", flow_run_name="subflow-{argument}")
def my_subflow(argument: str):
    logger = get_run_logger()
    logger.info(f"Subflow {argument} (starting)")
    subflow_task(argument)
    logger.info(f"Subflow {argument} (finished)")

@task
def main_task():
    logger = get_run_logger()
    stagger_seconds = 5
    arguments = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]
    sub_flow_runs = []
    for a in arguments:
        if sub_flow_runs:
            time.sleep(stagger_seconds)
        logger.info(f"About to trigger Subflow {a}")
        sub_flow_run = run_deployment(
            name="subflow/subflow_mytest",
            parameters={"argument": a},
            timeout=0,
        )
        sub_flow_runs.append(sub_flow_run)

    wait_for_flows_to_finish(sub_flow_runs)

@flow
def main():
    setup_sentry()
    logger = get_run_logger()
    logger.info("Starting main flow")
    main_task()
