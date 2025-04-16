from openai.types.responses import (
    FunctionToolParam,
)

from dbt_mcp.mcp.server import dbt_mcp


async def get_tools() -> list[FunctionToolParam]:
    mcp_tools = await dbt_mcp.list_tools()
    return [
        FunctionToolParam(
            type="function",
            name=t.name,
            description=t.description,
            parameters=t.inputSchema,
            strict=False,
        )
        for t in mcp_tools
    ]
