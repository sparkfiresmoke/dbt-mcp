import time

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from dbt_mcp.config.config import load_config
from dbt_mcp.semantic_layer.client import get_semantic_layer_fetcher

config = load_config()

# Create server parameters for stdio connection
server_params = StdioServerParameters(
    command="./.venv/bin/mcp",
    args=["run", "dbt_mcp/main.py"],
)


async def run():
    print("Starting test")
    start_time = time.time()
    async with (
        stdio_client(server_params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()

        await session.call_tool(
            "get_dimensions_truncated",
            arguments={"metrics": ["count_dbt_copilot_requests"]},
        )
        end_time = time.time()
        print(f"get_dimensions_truncated time: {end_time - start_time:.2f} seconds")

        await session.call_tool(
            "get_dimensions", arguments={"metrics": ["count_dbt_copilot_requests"]}
        )
        end_time = time.time()
        print(f"get_dimensions time: {end_time - start_time:.2f} seconds")

    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    start_time = time.time()
    semantic_layer_fetcher.get_dimensions(metrics=["count_dbt_copilot_requests"])
    end_time = time.time()
    print(f"Semantic layer dimensions fetch time: {end_time - start_time:.2f} seconds")

    start_time = time.time()
    semantic_layer_fetcher.list_metrics()
    end_time = time.time()
    print(f"Semantic layer metrics fetch time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
