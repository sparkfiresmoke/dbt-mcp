import asyncio
import json
from time import time

from openai import OpenAI
from openai.types.responses.response_input_param import FunctionCallOutput
from openai.types.responses.response_output_message import ResponseOutputMessage

from client.tools import get_tools
from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import dbt_mcp

LLM_MODEL = "gpt-4o-mini"
TOOL_RESPONSE_TRUNCATION = 100  # set to None for no truncation

llm_client = OpenAI()
config = load_config()
messages = []


async def main():
    user_role = "user"
    while True:
        user_input = input(f"{user_role} > ")
        messages.append({"role": user_role, "content": user_input})
        response_output = None
        tool_call_error = None
        while (
            response_output is None
            or response_output.type == "function_call"
            or tool_call_error is not None
        ):
            tool_call_error = None
            response = llm_client.responses.create(
                model=LLM_MODEL,
                input=messages,
                tools=await get_tools(),
                parallel_tool_calls=False,
            )
            response_output = response.output[0]
            if isinstance(response_output, ResponseOutputMessage):
                print(f"{response_output.role} > {response_output.content[0].text}")
            messages.append(response_output)
            if response_output.type != "function_call":
                continue
            print(
                f"Calling tool: {response_output.name} with arguments: {response_output.arguments}"
            )
            start_time = time()
            try:
                tool_response = await dbt_mcp.call_tool(
                    response_output.name,
                    json.loads(response_output.arguments),
                )
            except Exception as e:
                tool_call_error = e
                print(f"Error calling tool: {e}")
                messages.append(
                    FunctionCallOutput(
                        type="function_call_output",
                        call_id=response_output.call_id,
                        output=str(e),
                    )
                )
                continue
            tool_response_str = str(tool_response)
            print(
                f"Tool responded in {time() - start_time} seconds: "
                + (
                    f"{tool_response_str[:TOOL_RESPONSE_TRUNCATION]} [TRUNCATED]..."
                    if TOOL_RESPONSE_TRUNCATION
                    and len(tool_response_str) > TOOL_RESPONSE_TRUNCATION
                    else tool_response_str
                )
            )
            messages.append(
                FunctionCallOutput(
                    type="function_call_output",
                    call_id=response_output.call_id,
                    output=str(tool_response),
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
