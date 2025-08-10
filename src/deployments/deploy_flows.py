#!/usr/bin/env python
# deploy_flows.py

import asyncio
from pathlib import Path
import typer
from prefect import flow, __version__

# NOTE: The import path changed in 3.1.11
if tuple(map(int, __version__.split("."))) < (3, 1, 11):
    from prefect.deployments.base import _search_for_flow_functions
else:
    from prefect.cli._prompts import (
        search_for_flow_functions as _search_for_flow_functions,
    )

# Define overrides for each flow
OVERRIDES = {
    "src/flows/data_collection.py:collect_train_data_flow": {
        "work_pool_name": "my-pool",  # Use your work pool name
        "cron": "* * * * *",  # Every minute
    },
    "src/flows/data_processing.py:process_data_flow": {
        "work_pool_name": "my-pool",  # Use your work pool name
        "cron": "0 0 * * *",  # Daily
    }
}

# Maximum number of flows to deploy concurrently
MAX_CONCURRENCY = 2

async def deploy_flow(entrypoint: str, semaphore: asyncio.BoundedSemaphore):
    """
    Deploy a flow with common settings.
    """
    try:
        async with semaphore:
            print(f"Loading flow from {entrypoint}...")
            # Get flow from source
            _flow = await flow.from_source(
                source=Path.cwd(),  # Local filesystem source
                entrypoint=entrypoint,
            )
            
            # Get overrides for this flow
            overrides = OVERRIDES.get(entrypoint, {})
            
            # Add default overrides if not specified
            if "work_pool_name" not in overrides:
                overrides["work_pool_name"] = "my-pool"
            
            # Deploy the flow with overrides
            print(f"Deploying flow {entrypoint} with overrides: {overrides}")
            deployment_id = await _flow.deploy(
                name=entrypoint.split(":")[-1],
                **overrides
            )
            
        print(f"Successfully deployed flow {entrypoint} with deployment ID {deployment_id}")
        return deployment_id
    except Exception as e:
        print(f"Error deploying flow {entrypoint}: {e}")
        return e

async def filter_flow_functions(
    flow_functions: list[dict], include: list[str] = None, exclude: list[str] = None
) -> list[dict]:
    """
    Filter the flow functions based on the include and exclude options.
    """
    available = set([f["filepath"] for f in flow_functions])
    if include:
        missing = set(include) - available
        if any(missing):
            raise ValueError(
                f"The following included files were not found: {', '.join(str(m) for m in missing)}"
            )
        deployable = set(include)
    elif exclude:
        deployable = available - set(exclude)
    else:
        deployable = available

    return [f for f in flow_functions if f["filepath"] in deployable]

async def _deploy(
    path: Path = Path("./flows"), include: list[str] = None, exclude: list[str] = None
):
    """
    Search for flow functions in the specified directory and deploy each one.
    """
    print(f"Searching for flow functions in {path.absolute()}...")
    flow_functions = await _search_for_flow_functions(path)

    # Handle no flows found
    if not flow_functions:
        print(f"No flow functions found in the specified directory: {path.absolute()}")
        return

    print(f"Found {len(flow_functions)} flow functions: {[f['function_name'] for f in flow_functions]}")
    
    deployable = await filter_flow_functions(flow_functions, include, exclude)

    # Concurrency control
    semaphore = asyncio.BoundedSemaphore(MAX_CONCURRENCY)

    print(f"Deploying {len(deployable)} flows.")
    _deployments = []
    for flow_function in deployable:
        # Load and deploy the flow
        entrypoint = f"{flow_function['filepath']}:{flow_function['function_name']}"
        print(f"Preparing to deploy flow {flow_function['function_name']} from {flow_function['filepath']}")
        _deployment = deploy_flow(
            entrypoint=entrypoint,
            semaphore=semaphore,
        )
        _deployments.append(_deployment)

    # Wait for all deployments to complete
    results = await asyncio.gather(*_deployments, return_exceptions=True)

    # Summarize the results
    success_count = 0
    for idx, (flow_function, result) in enumerate(zip(deployable, results)):
        if isinstance(result, Exception):
            print(
                f"[{idx + 1}] ❌ {flow_function['filepath']}:{flow_function['function_name']}: {result}"
            )
        else:
            success_count += 1
            print(
                f"[{idx + 1}] ✅ {flow_function['filepath']}:{flow_function['function_name']}"
            )
    
    print(f"Successfully deployed {success_count} out of {len(deployable)} flows.")

def deploy(
    path: Path = typer.Argument(
        Path("./src/flows"),
        help="Directory path containing flow files to deploy",
    ),
    include: list[str] = typer.Option(
        None,
        "-i",
        "--include",
        help="Optional list of file names to include. If provided, only these files will be deployed",
    ),
    exclude: list[str] = typer.Option(
        None,
        "-e",
        "--exclude",
        help="Optional list of file names to exclude. If provided, these files will be skipped",
    ),
):
    if include and exclude:
        raise typer.BadParameter("Please provide either include or exclude, not both")

    asyncio.run(_deploy(path=path, include=include, exclude=exclude))

if __name__ == "__main__":
    typer.run(deploy)