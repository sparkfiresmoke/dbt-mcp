import logging
from typing import (
    Annotated,
    Any,
)

from httpx import Client
from mcp import CallToolRequest, JSONRPCResponse, ListToolsResult
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.tools.base import Tool
from mcp.server.fastmcp.utilities.func_metadata import (
    ArgModelBase,
    FuncMetadata,
    _get_typed_annotation,
)
from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    TextContent,
)
from mcp.types import Tool as RemoteTool
from pydantic import Field, ValidationError, WithJsonSchema, create_model
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from dbt_mcp.config.config import Config

logger = logging.getLogger(__name__)


# Based on this: https://github.com/modelcontextprotocol/python-sdk/blob/9ae4df85fbab97bf476ddd160b766ca4c208cd13/src/mcp/server/fastmcp/utilities/func_metadata.py#L105
def get_remote_tool_fn_metadata(tool: RemoteTool) -> FuncMetadata:
    dynamic_pydantic_model_params: dict[str, Any] = {}
    for key in tool.inputSchema["properties"].keys():
        # Remote tools shouldn't have type annotations or default values
        # for their arguments. So, we set them to defaults.
        field_info = FieldInfo.from_annotated_attribute(
            annotation=_get_typed_annotation(
                annotation=Annotated[
                    Any,
                    Field(),
                    WithJsonSchema({"title": key, "type": "string"}),
                ],
                globalns={},
            ),
            default=PydanticUndefined,
        )
        dynamic_pydantic_model_params[key] = (field_info.annotation, None)
    return FuncMetadata(
        arg_model=create_model(
            f"{tool.name}Arguments",
            **dynamic_pydantic_model_params,
            __base__=ArgModelBase,
        )
    )


def _get_remote_tools(config: Config, headers: dict[str, str]) -> list[RemoteTool]:
    try:
        with Client(base_url=config.remote_mcp_base_url, headers=headers) as client:
            list_tools_response = JSONRPCResponse.model_validate_json(
                client.get("/tools/list").text
            )
            return ListToolsResult.model_validate(list_tools_response.result).tools
    except Exception:
        return []


async def register_remote_tools(dbt_mcp: FastMCP, config: Config) -> None:
    assert config.dev_environment_id is not None
    assert config.user_id is not None
    headers = {
        "Authorization": f"Bearer {config.token}",
        "x-dbt-prod-environment-id": str(config.prod_environment_id),
        "x-dbt-dev-environment-id": str(config.dev_environment_id),
        "x-dbt-user-id": str(config.user_id),
    }
    for tool in _get_remote_tools(config=config, headers=headers):
        # Create a new function using a factory to avoid closure issues
        def create_tool_function(tool_name: str):
            async def tool_function(
                *args, **kwargs
            ) -> list[TextContent | ImageContent | EmbeddedResource]:
                with Client(
                    base_url=config.remote_mcp_base_url, headers=headers
                ) as client:
                    tool_call_http_response = client.post(
                        "/tools/call",
                        json=CallToolRequest(
                            method="tools/call",
                            params=CallToolRequestParams(
                                name=tool_name,
                                arguments=kwargs,
                            ),
                        ).model_dump(),
                    )
                    if tool_call_http_response.status_code != 200:
                        return [
                            TextContent(
                                type="text",
                                text=f"Failed to call tool {tool_name} with "
                                + f"status code: {tool_call_http_response.status_code} "
                                + f"error message: {tool_call_http_response.text}",
                            )
                        ]
                    try:
                        tool_call_jsonrpc_response = (
                            JSONRPCResponse.model_validate_json(
                                tool_call_http_response.text
                            )
                        )
                        tool_call_result = CallToolResult.model_validate(
                            tool_call_jsonrpc_response.result
                        )
                    except ValidationError as e:
                        return [
                            TextContent(
                                type="text",
                                text=f"Failed to parse tool response for {tool_name}: "
                                + f"{e}",
                            )
                        ]
                    if tool_call_result.isError:
                        return [
                            TextContent(
                                type="text",
                                text=f"Tool {tool_name} reported an error: "
                                + f"{tool_call_result.content}",
                            )
                        ]
                    return tool_call_result.content

            return tool_function

        new_tool = create_tool_function(tool.name)
        dbt_mcp._tool_manager._tools[tool.name] = Tool(
            fn=new_tool,
            name=tool.name,
            description=tool.description or "",
            parameters=tool.inputSchema,
            fn_metadata=get_remote_tool_fn_metadata(tool),
            is_async=True,
            context_kwarg=None,
        )
