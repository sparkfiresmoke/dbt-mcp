import time
from functools import cache

import requests

from dbt_mcp.config.config import Config
from dbt_mcp.semantic_layer.gql.gql import GRAPHQL_QUERIES
from dbt_mcp.semantic_layer.gql.gql_request import ConnAttr, submit_request
from dbt_mcp.semantic_layer.levenshtein import get_misspellings
from dbt_mcp.semantic_layer.types import (
    DimensionToolResponse,
    EntityToolResponse,
    MetricToolResponse,
)


class SemanticLayerFetcher:
    def __init__(self, host: str, config: Config):
        self.host = host
        self.config = config
        self.entities_cache: dict[str, list[EntityToolResponse]] = {}
        self.dimensions_cache: dict[str, list[DimensionToolResponse]] = {}

    @cache
    def list_metrics(self) -> list[MetricToolResponse]:
        metrics_result = submit_request(
            ConnAttr(
                host=self.host,
                params={"environmentid": self.config.prod_environment_id},
                auth_header=f"Bearer {self.config.token}",
            ),
            {"query": GRAPHQL_QUERIES["metrics"]},
        )
        if "errors" in metrics_result:
            raise ValueError(metrics_result["errors"])
        return [
            MetricToolResponse(
                name=m.get("name"),
                type=m.get("type"),
                label=m.get("label"),
                description=m.get("description"),
            )
            for m in metrics_result["data"]["metrics"]
        ]

    def get_dimensions(self, metrics: list[str]) -> list[DimensionToolResponse]:
        metrics_key = ",".join(sorted(metrics))
        if metrics_key not in self.dimensions_cache:
            dimensions_result = submit_request(
                ConnAttr(
                    host=self.host,
                    params={"environmentid": self.config.prod_environment_id},
                    auth_header=f"Bearer {self.config.token}",
                ),
                {
                    "query": GRAPHQL_QUERIES["dimensions"],
                    "variables": {"metrics": [{"name": m} for m in metrics]},
                },
            )
            if "errors" in dimensions_result:
                raise ValueError(dimensions_result["errors"])
            dimensions = []
            for d in dimensions_result["data"]["dimensions"]:
                dimensions.append(
                    DimensionToolResponse(
                        name=d.get("name"),
                        type=d.get("type"),
                        description=d.get("description"),
                        label=d.get("label"),
                        granularities=d.get("queryableGranularities")
                        + d.get("queryableTimeGranularities"),
                    )
                )
            self.dimensions_cache[metrics_key] = dimensions
        return self.dimensions_cache[metrics_key]

    def get_entities(self, metrics: list[str]) -> list[EntityToolResponse]:
        metrics_key = ",".join(sorted(metrics))
        if metrics_key not in self.entities_cache:
            entities_result = submit_request(
                ConnAttr(
                    host=self.host,
                    params={"environmentid": self.config.prod_environment_id},
                    auth_header=f"Bearer {self.config.token}",
                ),
                {
                    "query": GRAPHQL_QUERIES["entities"],
                    "variables": {"metrics": [{"name": m} for m in metrics]},
                },
            )
            if "errors" in entities_result:
                raise ValueError(entities_result["errors"])
            entities = [
                EntityToolResponse(
                    name=e.get("name"),
                    type=e.get("type"),
                    description=e.get("description"),
                )
                for e in entities_result["data"]["entities"]
            ]
            self.entities_cache[metrics_key] = entities
        return self.entities_cache[metrics_key]

    def validate_query_metrics_params(
        self, metrics: list[str], group_by: list[str] | None
    ) -> str | None:
        errors = []
        available_metrics_names = [m.name for m in self.list_metrics()]
        metric_misspellings = get_misspellings(
            targets=metrics,
            words=available_metrics_names,
            top_k=5,
        )
        for metric_misspelling in metric_misspellings:
            recommendations = (
                " Did you mean: " + ", ".join(metric_misspelling.similar_words) + "?"
            )
            errors.append(
                f"Metric {metric_misspelling.word} not found." + recommendations
                if metric_misspelling.similar_words
                else ""
            )

        if errors:
            return f"Errors: {', '.join(errors)}"

        available_dimensions = [d.name for d in self.get_dimensions(metrics)]
        dimension_misspellings = get_misspellings(
            targets=group_by or [],
            words=available_dimensions,
            top_k=5,
        )
        for dimension_misspelling in dimension_misspellings:
            recommendations = (
                " Did you mean: " + ", ".join(dimension_misspelling.similar_words) + "?"
            )
            errors.append(
                f"Dimension {dimension_misspelling.word} not found." + recommendations
                if dimension_misspelling.similar_words
                else ""
            )

        if errors:
            return f"Errors: {', '.join(errors)}"
        return None

    def query_metrics(
        self,
        metrics: list[str],
        group_by: list[str] | None = None,
        time_grain: str | None = None,
        limit: int | None = None,
    ):
        error_message = self.validate_query_metrics_params(
            metrics=metrics,
            group_by=group_by,
        )
        if error_message:
            return error_message

        # Generate metric list string for GraphQL
        metric_list = ", ".join([f'{{name: "{metric}"}}' for metric in metrics])

        # Build group_by section if needed
        group_by_section = ""
        if group_by:
            groups = []
            for dim in group_by:
                if dim == "metric_time" and time_grain:
                    groups.append(f'{{name: "{dim}", grain: {time_grain}}}')
                else:
                    groups.append(f'{{name: "{dim}"}}')
            group_by_section = f"groupBy: [{', '.join(groups)}]"

        # Build limit section if needed
        limit_section = f"limit: {limit}" if limit else ""

        # Build create query mutation with direct string construction
        mutation = f"""
        mutation {{
        createQuery(
            environmentId: "{self.config.prod_environment_id}"
            metrics: [{metric_list}]
            {group_by_section}
            {limit_section}
        ) {{
            queryId
        }}
        }}
        """

        url = f"{self.host}/api/graphql"
        headers = {"Authorization": f"Bearer {self.config.token}"}

        # Execute create query mutation
        response = requests.post(url, headers=headers, json={"query": mutation})
        response.raise_for_status()
        create_data = response.json()

        if "errors" in create_data:
            return f"GraphQL error: {create_data['errors']}"

        # Get query ID
        query_id = create_data["data"]["createQuery"]["queryId"]

        # Poll for results
        max_attempts = 30
        attempts = 0
        query_result = None

        while attempts < max_attempts:
            attempts += 1

            # Query for results
            result_query = f"""
            {{
            query(environmentId: "{self.config.prod_environment_id}", queryId: "{query_id}") {{
                status
                error
                jsonResult(encoded: false)
            }}
            }}
            """

            response = requests.post(url, headers=headers, json={"query": result_query})
            response.raise_for_status()
            result_data = response.json()

            if "errors" in result_data:
                return f"GraphQL error: {result_data['errors']}"

            query_result = result_data["data"]["query"]

            # Check status
            if query_result["status"] == "FAILED":
                return f"Query failed: {query_result['error']}"
            elif query_result["status"] == "SUCCESSFUL":
                break

            # Wait before polling again
            time.sleep(1)

        if attempts >= max_attempts:
            return "Query timed out. Please try again or simplify your query."

        # Parse and return results
        if query_result and query_result.get("jsonResult"):
            # Return the raw JSON result
            return query_result["jsonResult"]
        else:
            return "No results returned."


def get_semantic_layer_fetcher(config: Config) -> SemanticLayerFetcher:
    if config.host and config.host.startswith("localhost"):
        host = f"http://{config.host}"
    elif config.multicell_account_prefix:
        host = f"https://{config.multicell_account_prefix}.semantic-layer.{config.host}"
    else:
        host = f"https://semantic-layer.{config.host}"
    if config.prod_environment_id is None:
        raise ValueError("Environment ID is required")
    if config.token is None:
        raise ValueError("Token is required")

    return SemanticLayerFetcher(
        host=host,
        config=config,
    )
