import asyncio
from time import time

from tqdm import tqdm

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import dbt_mcp
from dbt_mcp.semantic_layer.client import get_semantic_layer_fetcher

config = load_config()
semantic_layer_fetcher = get_semantic_layer_fetcher(config)


async def get_direct_call_latency() -> float:
    start_time = time()
    semantic_layer_fetcher.list_metrics()
    end_time = time()
    return end_time - start_time


async def get_mcp_call_latency() -> float:
    start_time = time()
    await dbt_mcp.call_tool("list_metrics", {})
    end_time = time()
    return end_time - start_time


async def main():
    latencies = []
    for _ in tqdm(range(100), desc="Measuring latency"):
        latencies.append(await get_direct_call_latency())

    print(f"Direct call Average latency: {sum(latencies) / len(latencies)}")
    # Calculate p90 latency
    latencies.sort()
    p90_index = int(len(latencies) * 0.9)
    print(f"Direct call P90 latency: {latencies[p90_index]}")

    latencies = []
    for _ in tqdm(range(100), desc="Measuring latency"):
        latencies.append(await get_mcp_call_latency())

    print(f"MCP Average latency: {sum(latencies) / len(latencies)}")
    # Calculate p90 latency
    latencies.sort()
    p90_index = int(len(latencies) * 0.9)
    print(f"MCP P90 latency: {latencies[p90_index]}")


if __name__ == "__main__":
    asyncio.run(main())
