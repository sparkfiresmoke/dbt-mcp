from mcp.server.fastmcp import FastMCP
from config.config import Config
from discovery.client import MetadataAPIClient, ModelsFetcher


def register_discovery_tools(dbt_mcp: FastMCP, config: Config) -> None:
    api_client = MetadataAPIClient(config.host, config.token)
    models_fetcher = ModelsFetcher(api_client, config.environment_id)

    @dbt_mcp.tool()
    def get_mart_models() -> list[dict]:
        """
        A mart model is part of the presentation layer of the dbt project. It's where cleaned, transformed data is organized for consumption by end-users, like analysts, dashboards, or business tools.
        """
        mart_models = models_fetcher.fetch_models(model_filter={"modelingLayer": "marts"})
        return [m for m in mart_models if m["name"] != "metricflow_time_spine"]
