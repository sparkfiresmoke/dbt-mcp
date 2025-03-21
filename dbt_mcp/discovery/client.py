import textwrap
from typing import Any, Optional, TypedDict, Literal
import requests

PAGE_SIZE = 100

class GraphQLQueries:
    GET_MODELS = textwrap.dedent("""
        query GetModels(
            $environmentId: BigInt!,
            $modelsFilter: ModelAppliedFilter,
            $after: String,
            $first: Int
        ) {
            environment(id: $environmentId) {
                applied {
                    models(filter: $modelsFilter, after: $after, first: $first) {
                        pageInfo {
                            endCursor
                        }
                        edges {
                            node {
                                name
                                compiledCode
                                description
                                catalog {
                                    columns {
                                        description
                                        name
                                        type
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    """)

class MetadataAPIClient:
    def __init__(self, host: str, token: str):
        self.url = f'https://metadata.{host}/graphql'
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        }

    def execute_query(self, query: str, variables: dict) -> dict:
        response = requests.post(
            url=self.url,
            json={"query": query, "variables": variables},
            headers=self.headers
        )
        return response.json()

class ModelFilter(TypedDict, total=False):
    access: Optional[Literal["public", "private", "protected"]]
    modelingLayer: Optional[Literal["marts", "fact", "dimension", "fct", "dim"]]
    group: Optional[str]

class ModelsFetcher:
    def __init__(self, api_client: MetadataAPIClient, environment_id: int):
        self.api_client = api_client
        self.environment_id = environment_id

    def _parse_response_to_json(self, result: dict) -> list[dict]:
        edges = result["data"]["environment"]["applied"]["models"]["edges"]
        parsed_edges: list[dict] = []
        if not edges:
            return parsed_edges
        if result.get("errors"):
            raise Exception(f"GraphQL query failed: {result['errors']}")
        for edge in edges:
            if not isinstance(edge, dict) or "node" not in edge:
                continue
            node = edge["node"]
            if not isinstance(node, dict):
                continue
            parsed_edges.append(node)
        return parsed_edges

    def fetch_models(self, model_filter: Optional[ModelFilter] = None) -> list[dict]:
        has_next_page = True
        after_cursor: str = ""
        all_edges: list[dict] = []

        while has_next_page:
            variables = {
                "environmentId": self.environment_id,
                "after": after_cursor,
                "first": PAGE_SIZE,
                "modelsFilter": model_filter or {}
            }

            result = self.api_client.execute_query(GraphQLQueries.GET_MODELS, variables)
            all_edges.extend(self._parse_response_to_json(result))

            previous_after_cursor = after_cursor
            after_cursor = result['data']['environment']['applied']['models']['pageInfo']['endCursor']
            if previous_after_cursor == after_cursor:
                has_next_page = False

        return all_edges