import time

from adbc_driver_flightsql import DatabaseOptions
from adbc_driver_flightsql.dbapi import connect
from dbtsl import SemanticLayerClient

from dbt_mcp.config.config import load_config
from dbt_mcp.iris.gql import GRAPHQL_QUERIES
from dbt_mcp.iris.iris import ConnAttr, submit_request
from dbt_mcp.semantic_layer.client import get_semantic_layer_fetcher

config = load_config()


def test_semantic_layer_list_metrics():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    metrics = semantic_layer_fetcher.list_metrics()
    assert len(metrics) > 0


def test_sdk_metrics():
    start_time = time.time()
    client = SemanticLayerClient(
        environment_id=config.environment_id,
        auth_token=config.token,
        host=f"semantic-layer.{config.host}",
    )
    with client.session():
        metrics = client.metrics()
        print(metrics)
    end_time = time.time()
    execution_time = end_time - start_time
    raise ValueError(f"SDK metrics fetch took {execution_time:.2f} seconds")


def test_without_granularities():
    start_time = time.time()
    submit_request(
        ConnAttr(
            host=f"https://semantic-layer.{config.host}",
            params={"environmentid": config.environment_id},
            auth_header=f"Bearer {config.token}",
        ),
        {"query": GRAPHQL_QUERIES["metrics"]},
    )
    end_time = time.time()
    execution_time = end_time - start_time
    raise ValueError(f"Metrics fetch took {execution_time:.2f} seconds")


def test_adbc():
    with (
        connect(
            f"grpc+tls://{config.host}:433",
            db_kwargs={
                DatabaseOptions.AUTHORIZATION_HEADER.value: f"Bearer {config.token}",
                f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}environmentid": config.environment_id,
                DatabaseOptions.WITH_COOKIE_MIDDLEWARE.value: "true",
            },
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute("select * from {{ semantic_layer.metrics() }}")
        df = cur.fetch_df()  # fetches as Pandas DF, can also do fetch_arrow_table
    print(df.to_string())


def test_dimensions_query_sdk():
    start_time = time.time()
    client = SemanticLayerClient(
        environment_id=config.environment_id,
        auth_token=config.token,
        host=f"semantic-layer.{config.host}",
    )
    with client.session():
        dimensions = client.dimensions(metrics=["count_dbt_copilot_requests"])
        print(dimensions)
    end_time = time.time()
    execution_time = end_time - start_time
    raise ValueError(f"SDK dimensions fetch took {execution_time:.2f} seconds")


def test_dimensions_fetcher():
    start_time = time.time()
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    dimensions = semantic_layer_fetcher.get_dimensions(
        metrics=["count_dbt_copilot_requests"]
    )
    print(dimensions)
    end_time = time.time()
    execution_time = end_time - start_time
    raise ValueError(f"Fetch dimensions fetch took {execution_time:.2f} seconds")


def test_semantic_layer_list_dimensions():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    metrics = semantic_layer_fetcher.list_metrics()
    dimensions = semantic_layer_fetcher.get_dimensions(metrics=[metrics[0].name])
    assert len(dimensions) > 0


def test_semantic_layer_query_metrics():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    metrics = semantic_layer_fetcher.list_metrics()
    result = semantic_layer_fetcher.query_metrics(metrics=[metrics[0].name])
    assert result is not None


def test_semantic_layer_query_metrics_with_misspellings():
    semantic_layer_fetcher = get_semantic_layer_fetcher(config)
    result = semantic_layer_fetcher.query_metrics(["revehue"])
    assert result is not None
    assert "revenue" in result
