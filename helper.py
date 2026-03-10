import asyncio
from prefect.exceptions import FlowRunWaitTimeout
from prefect.client.schemas import FlowRun
import uuid
import time
from prefect import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.states import Cancelled
from prefect.logging import get_run_logger


def wait_for_flows_to_finish(
    flow_runs: list[FlowRun],
    timeout: float = 12 * 60 * 60,  # 12 hours
    settle_seconds: float = 0,
    crashed_grace_seconds: float = 5 * 60,  # 5 minutes
) -> list[FlowRun]:
    """
    Wait for list of flow runs to finish running.

    Uses a single bulk API call per poll cycle (read_flow_runs with ID filter)
    instead of one call per flow run.

    Crashed flows are kept in the poll list for ``crashed_grace_seconds`` — they
    can recover in Prefect Cloud.  If still Crashed after the grace period, they
    are treated as failed.

    Failed/Cancelled flows are logged as warnings but do not raise.

    Returns the final FlowRun objects for all settled flows.

    Parameters:
        flow_runs: list of flow runs to wait for
        timeout: timeout in seconds
        settle_seconds: initial delay before polling to let flows start up
        crashed_grace_seconds: how long to keep polling a Crashed flow before
            treating it as failed
    """

    logger = get_run_logger()
    pending_ids = {f.id for f in flow_runs}
    finished_runs: list[FlowRun] = []
    first_crashed_at: dict[uuid.UUID, float] = {}
    start_time = time.monotonic()

    if not pending_ids:
        return []

    logger.info(f"Waiting for {len(pending_ids)} flows to finish")

    async def wait():
        await asyncio.sleep(settle_seconds)

        while pending_ids:
            async with get_client() as client:
                runs = await client.read_flow_runs(
                    flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=list(pending_ids)))
                )
                now = time.monotonic()
                for run in runs:
                    logger.info(f"Flow run {run.id} state is {run.state}")
                    if run.state and run.state.is_completed():
                        pending_ids.discard(run.id)
                        first_crashed_at.pop(run.id, None)
                        finished_runs.append(run)
                        logger.info(f"Flow run {run.id} completed")
                    elif run.state and run.state.is_failed():
                        pending_ids.discard(run.id)
                        finished_runs.append(run)
                        logger.warning(f"Flow run {run.id} failed")
                    elif run.state and run.state.is_cancelled():
                        pending_ids.discard(run.id)
                        finished_runs.append(run)
                        logger.warning(f"Flow run {run.id} cancelled")
                    elif run.state and run.state.is_crashed():
                        if run.id not in first_crashed_at:
                            first_crashed_at[run.id] = now
                            logger.warning(f"Flow run {run.id} crashed, will keep polling for {crashed_grace_seconds}s")
                        elif now - first_crashed_at[run.id] > crashed_grace_seconds:
                            pending_ids.discard(run.id)
                            finished_runs.append(run)
                            logger.warning(f"Flow run {run.id} still crashed after grace period, treating as failed")
                    # Other non-terminal states: keep polling

                if time.monotonic() > start_time + timeout:
                    for fid in list(pending_ids):
                        await client.set_flow_run_state(fid, Cancelled())
                    msg = (
                        f"{len(pending_ids)} flows exceeded timeout ({timeout} seconds): "
                        f"({', '.join([str(fid) for fid in pending_ids])})"
                    )
                    raise FlowRunWaitTimeout(msg)

            await asyncio.sleep(5)

    asyncio.run(wait())

    failed = [r for r in finished_runs if r.state and not r.state.is_completed()]
    if failed:
        msg = ", ".join(f"{r.id}: {r.state.name}" for r in failed if r.state)
        logger.warning(f"Some flow runs did not complete successfully: {msg}")

    return finished_runs