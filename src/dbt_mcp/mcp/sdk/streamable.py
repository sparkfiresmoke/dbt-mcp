import logging
from contextlib import asynccontextmanager
from typing import Any

import anyio
import httpx
from httpx_sse import EventSource
from mcp import types
from pydantic import TypeAdapter

logger = logging.getLogger(__name__)


@asynccontextmanager
async def streamable_client(
    url: str,
    headers: dict[str, Any] | None = None,
    timeout: float = 5,
):
    """
    Client transport for streamable HTTP.
    """
    read_stream_writer, read_stream = anyio.create_memory_object_stream[
        types.JSONRPCMessage | Exception
    ](0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream[
        types.JSONRPCMessage
    ](0)

    async def handle_response(text: str) -> None:
        items = _maybe_list_adapter.validate_json(text)
        if isinstance(items, types.JSONRPCMessage):
            items = [items]
        for item in items:
            await read_stream_writer.send(item)

    session_headers = headers.copy() if headers else {}

    async with anyio.create_task_group() as tg:
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:

                async def sse_reader(event_source: EventSource):
                    try:
                        async for sse in event_source.aiter_sse():
                            logger.debug(f"Received SSE event: {sse.event}")
                            match sse.event:
                                case "message":
                                    try:
                                        await handle_response(sse.data)
                                        logger.debug(
                                            f"Received server message: {sse.data}"
                                        )
                                    except Exception as exc:
                                        logger.error(
                                            f"Error parsing server message: {exc}"
                                        )
                                        await read_stream_writer.send(exc)
                                        continue
                                case _:
                                    logger.warning(f"Unknown SSE event: {sse.event}")
                    except Exception as exc:
                        logger.error(f"Error in sse_reader: {exc}")
                        await read_stream_writer.send(exc)
                    finally:
                        await read_stream_writer.aclose()

                async def post_writer():
                    nonlocal headers
                    try:
                        async with write_stream_reader:
                            async for message in write_stream_reader:
                                logger.debug(f"Sending client message: {message}")
                                response = await client.post(
                                    url,
                                    json=message.model_dump(
                                        by_alias=True,
                                        mode="json",
                                        exclude_none=True,
                                    ),
                                    headers=(
                                        ("accept", "application/json"),
                                        ("accept", "text/event-stream"),
                                        *session_headers.items(),
                                    ),
                                )
                                content_type = response.headers.get("content-type")
                                logger.debug(
                                    f"response {url=} "
                                    f"content-type={content_type} "
                                    f"body={response.text}"
                                )

                                response.raise_for_status()
                                match response.headers.get("mcp-session-id"):
                                    case str() as session_id:
                                        session_headers["mcp-session-id"] = session_id
                                    case _:
                                        pass

                                match response.headers.get("content-type"):
                                    case "text/event-stream":
                                        await sse_reader(EventSource(response))
                                    case "application/json":
                                        await handle_response(response.text)
                                    case None:
                                        pass
                                    case unknown:
                                        logger.warning(
                                            f"Unknown content type: {unknown}"
                                        )

                                logger.debug(
                                    "Client message sent successfully: "
                                    f"{response.status_code}"
                                )
                    except Exception as exc:
                        logger.error(f"Error in post_writer: {exc}", exc_info=True)
                    finally:
                        await write_stream.aclose()

                tg.start_soon(post_writer)

                try:
                    yield read_stream, write_stream
                finally:
                    tg.cancel_scope.cancel()
        finally:
            await read_stream_writer.aclose()
            await write_stream.aclose()


_maybe_list_adapter: TypeAdapter[types.JSONRPCMessage | list[types.JSONRPCMessage]] = (
    TypeAdapter(types.JSONRPCMessage | list[types.JSONRPCMessage])
)
