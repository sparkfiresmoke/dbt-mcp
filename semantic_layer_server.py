#!/usr/bin/env python
"""
MCP Server for dbt Semantic Layer API

This module provides tools for interacting with the dbt Semantic Layer API,
including metric discovery, dimension analysis, and query management.
"""

import json
import requests
from mcp.server.fastmcp import FastMCP
from dbt_api_utils import (
    CONFIG, connect, auto_connect, execute_query, 
    execute_mutation, format_json_response, get_platform_dialect
)

# Create the MCP server
mcp = FastMCP("dbt Semantic Layer API")

@mcp.tool()
def connect_to_semantic_layer(host: str, token: str, environment_id: str) -> str:
    """
    Connect to the dbt Semantic Layer API.
    
    Args:
        host: The dbt API host
        token: The authentication token
        environment_id: The environment ID for semantic layer operations
        
    Returns:
        str: Success or error message
    """
    if connect(host, token, environment_id):
        return "Successfully connected to the semantic layer"
    return "Failed to connect to the semantic layer"

@mcp.tool()
def list_metrics():
    """
    List all available metrics in the dbt project with their basic information.
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
        }}
        queryableGranularities
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    return json.dumps(result["metrics"], indent=2)

@mcp.tool()
def get_metric_details(metric_name):
    """
    Get detailed information about a specific metric.
    
    Args:
        metric_name: The name of the metric to get details for
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          description
          type
          typeParams {{
            timeGranularity
          }}
          isPartition
          expr
          queryableGranularities
        }}
        queryableGranularities
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            return json.dumps(metric, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_metrics_by_type(metric_type):
    """
    Find all metrics of a specific type (simple, ratio, cumulative, derived).
    
    Args:
        metric_type: The type of metrics to find (SIMPLE, RATIO, CUMULATIVE, DERIVED)
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter metrics by type
    filtered_metrics = [m for m in result["metrics"] if m["type"] == metric_type.upper()]
    return json.dumps(filtered_metrics, indent=2)

@mcp.tool()
def get_metric_measures(metric_name):
    """
    Get all measures associated with a specific metric.
    
    Args:
        metric_name: The name of the metric to get measures for
    """
    query = f"""{{
      measures(environmentId: "{CONFIG['environment_id']}", metrics: [{{name: "{metric_name}"}}]) {{
        name
        aggTimeDimension
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    return json.dumps(result["measures"], indent=2)

@mcp.tool()
def find_metrics_by_domain(domain):
    """
    Find metrics that belong to a specific business domain or context.
    
    Args:
        domain: The business domain to search for in metric descriptions
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter metrics by domain in description
    filtered_metrics = [
        m for m in result["metrics"] 
        if m["description"] and domain.lower() in m["description"].lower()
    ]
    return json.dumps(filtered_metrics, indent=2)

@mcp.tool()
def get_metric_configurations(metric_name):
    """
    Get detailed configuration information for a specific metric.
    
    Args:
        metric_name: The name of the metric to get configurations for
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            return json.dumps(metric, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_critical_metrics():
    """
    Find metrics that are part of critical business processes by analyzing their usage patterns.
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Identify critical metrics based on:
    # 1. Having a description (indicating business context)
    # 2. Having multiple dimensions (indicating complex business logic)
    # 3. Being of type RATIO or DERIVED (indicating business calculations)
    critical_metrics = [
        m for m in result["metrics"]
        if (m["description"] and 
            len(m["dimensions"]) > 1 and 
            m["type"] in ["RATIO", "DERIVED"])
    ]
    
    return json.dumps(critical_metrics, indent=2)

@mcp.tool()
def list_dimensions(metric_name=None):
    """
    List all available dimensions, optionally filtered by metric.
    
    Args:
        metric_name: Optional metric name to filter dimensions by
    """
    query = f"""{{
      dimensions(environmentId: "{CONFIG['environment_id']}"{f', metrics: [{{name: "{metric_name}"}}]' if metric_name else ''}) {{
        name
        description
        type
        typeParams {{
          timeGranularity
        }}
        isPartition
        expr
        queryableGranularities
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    return json.dumps(result["dimensions"], indent=2)

@mcp.tool()
def find_dimensions_by_type(dimension_type):
    """
    Find all dimensions of a specific type (categorical, time).
    
    Args:
        dimension_type: The type of dimensions to find (CATEGORICAL, TIME)
    """
    query = f"""{{
      dimensions(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        typeParams {{
          timeGranularity
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter dimensions by type
    filtered_dimensions = [d for d in result["dimensions"] if d["type"] == dimension_type.upper()]
    return json.dumps(filtered_dimensions, indent=2)

@mcp.tool()
def get_dimension_granularities(dimension_name):
    """
    Get available time granularities for a specific time dimension.
    
    Args:
        dimension_name: The name of the time dimension
    """
    query = f"""{{
      dimensions(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        queryableGranularities
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific dimension
    for dimension in result["dimensions"]:
        if dimension["name"] == dimension_name:
            if dimension["type"] != "TIME":
                return f"Dimension '{dimension_name}' is not a time dimension"
            return json.dumps(dimension["queryableGranularities"], indent=2)
    
    return f"Dimension '{dimension_name}' not found"

@mcp.tool()
def get_semantic_model_dimensions(semantic_model_name):
    """
    Get all dimensions that are part of a specific semantic model.
    
    Args:
        semantic_model_name: The name of the semantic model
    """
    query = f"""{{
      semanticModels(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          description
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific semantic model
    for model in result["semanticModels"]:
        if model["name"] == semantic_model_name:
            return json.dumps(model["dimensions"], indent=2)
    
    return f"Semantic model '{semantic_model_name}' not found"

@mcp.tool()
def find_shared_dimensions(metric_names):
    """
    Find dimensions that are shared across multiple metrics.
    
    Args:
        metric_names: List of metric names to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter metrics to only those requested
    metrics = [m for m in result["metrics"] if m["name"] in metric_names]
    if not metrics:
        return "No metrics found with the provided names"
    
    # Find shared dimensions
    dimension_counts = {}
    for metric in metrics:
        for dimension in metric["dimensions"]:
            dim_name = dimension["name"]
            if dim_name not in dimension_counts:
                dimension_counts[dim_name] = {
                    "count": 0,
                    "type": dimension["type"],
                    "metrics": []
                }
            dimension_counts[dim_name]["count"] += 1
            dimension_counts[dim_name]["metrics"].append(metric["name"])
    
    # Filter for dimensions used in multiple metrics
    shared_dimensions = {
        name: info for name, info in dimension_counts.items()
        if info["count"] > 1
    }
    
    return json.dumps(shared_dimensions, indent=2)

@mcp.tool()
def find_dimensions_by_context(context):
    """
    Find dimensions with specific business context in their descriptions.
    
    Args:
        context: The business context to search for in dimension descriptions
    """
    query = f"""{{
      dimensions(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        typeParams {{
          timeGranularity
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter dimensions by context in description
    filtered_dimensions = [
        d for d in result["dimensions"]
        if d["description"] and context.lower() in d["description"].lower()
    ]
    return json.dumps(filtered_dimensions, indent=2)

@mcp.tool()
def find_partition_dimensions():
    """
    Find all dimensions that are marked as partition dimensions.
    """
    query = f"""{{
      dimensions(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        isPartition
        typeParams {{
          timeGranularity
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter for partition dimensions
    partition_dimensions = [d for d in result["dimensions"] if d["isPartition"]]
    return json.dumps(partition_dimensions, indent=2)

@mcp.tool()
def find_custom_dimensions():
    """
    Find dimensions that have custom expressions defined.
    """
    query = f"""{{
      dimensions(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        expr
        typeParams {{
          timeGranularity
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter for dimensions with custom expressions
    custom_dimensions = [d for d in result["dimensions"] if d["expr"]]
    return json.dumps(custom_dimensions, indent=2)

@mcp.tool()
def get_dimension_type_params(dimension_name):
    """
    Get detailed type parameters for a specific dimension.
    
    Args:
        dimension_name: The name of the dimension to get parameters for
    """
    query = f"""{{
      dimensions(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          timeGranularity
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific dimension
    for dimension in result["dimensions"]:
        if dimension["name"] == dimension_name:
            return json.dumps(dimension["typeParams"], indent=2)
    
    return f"Dimension '{dimension_name}' not found"

@mcp.tool()
def query_metrics_by_time_grain(metric_names, time_grain, start_date=None, end_date=None):
    """
    Query metrics with a specific time grain (day, week, month, quarter, year).
    
    Args:
        metric_names: List of metric names to query
        time_grain: The time grain to use (DAY, WEEK, MONTH, QUARTER, YEAR)
        start_date: Optional start date in ISO format (YYYY-MM-DD)
        end_date: Optional end date in ISO format (YYYY-MM-DD)
    """
    # Ensure metrics is a list
    if isinstance(metric_names, str):
        metric_names = [metric_names]
    
    # Generate metric list string for GraphQL
    metric_list = ", ".join([f"{{name: \"{metric}\"}}" for metric in metric_names])
    
    # Build time filter if dates are provided
    time_filter = ""
    if start_date and end_date:
        time_filter = f', timeFilter: {{start: "{start_date}", end: "{end_date}"}}'
    
    mutation = f"""
    mutation {{
      createQuery(
        environmentId: "{CONFIG['environment_id']}"
        metrics: [{metric_list}]
        groupBy: [{{name: "metric_time", grain: {time_grain}}}]
        {time_filter}
      ) {{
        queryId
      }}
    }}
    """
    
    result = execute_mutation(mutation)
    if "error" in result:
        return result["error"]
    
    # Get query ID and poll for results
    query_id = result["createQuery"]["queryId"]
    return poll_query_results(query_id)

@mcp.tool()
def analyze_metrics_across_periods(metric_names, time_grain, periods):
    """
    Analyze metrics across different time periods.
    
    Args:
        metric_names: List of metric names to analyze
        time_grain: The time grain to use (DAY, WEEK, MONTH, QUARTER, YEAR)
        periods: List of period names to analyze (e.g., ["last_30_days", "last_90_days"])
    """
    results = {}
    for period in periods:
        # Calculate date range based on period
        end_date = time.strftime("%Y-%m-%d")
        if period == "last_30_days":
            start_date = time.strftime("%Y-%m-%d", time.localtime(time.time() - 30*24*60*60))
        elif period == "last_90_days":
            start_date = time.strftime("%Y-%m-%d", time.localtime(time.time() - 90*24*60*60))
        elif period == "last_quarter":
            # Calculate last quarter's start and end dates
            current_month = time.localtime().tm_mon
            quarter_start = ((current_month - 1) // 3) * 3 + 1
            if quarter_start == 1:
                start_date = time.strftime("%Y-01-01")
            elif quarter_start == 4:
                start_date = time.strftime("%Y-04-01")
            elif quarter_start == 7:
                start_date = time.strftime("%Y-07-01")
            else:
                start_date = time.strftime("%Y-10-01")
        else:
            return f"Unsupported period: {period}"
        
        # Query metrics for this period
        period_result = query_metrics_by_time_grain(
            metric_names, time_grain, start_date, end_date
        )
        results[period] = period_result
    
    return json.dumps(results, indent=2)

@mcp.tool()
def compare_metrics_across_time_dimensions(metric_names, time_dimensions):
    """
    Compare metrics across different time dimensions.
    
    Args:
        metric_names: List of metric names to compare
        time_dimensions: List of time dimension names to compare across
    """
    results = {}
    for dimension in time_dimensions:
        # Generate metric list string for GraphQL
        metric_list = ", ".join([f"{{name: \"{metric}\"}}" for metric in metric_names])
        
        mutation = f"""
        mutation {{
          createQuery(
            environmentId: "{CONFIG['environment_id']}"
            metrics: [{metric_list}]
            groupBy: [{{name: "{dimension}"}}]
          ) {{
            queryId
          }}
        }}
        """
        
        result = execute_mutation(mutation)
        if "error" in result:
            results[dimension] = f"Error: {result['error']}"
            continue
        
        # Get query ID and poll for results
        query_id = result["createQuery"]["queryId"]
        results[dimension] = poll_query_results(query_id)
    
    return json.dumps(results, indent=2)

@mcp.tool()
def find_time_based_aggregations(metric_name):
    """
    Find metrics with specific time-based aggregations.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          window
          grainToDate
        }}
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Extract time-based configuration
            time_config = {
                "window": metric["typeParams"].get("window"),
                "grain_to_date": metric["typeParams"].get("grainToDate"),
                "time_dimensions": [
                    {
                        "name": d["name"],
                        "granularity": d["typeParams"].get("timeGranularity")
                    }
                    for d in metric["dimensions"]
                    if d["type"] == "TIME"
                ]
            }
            return json.dumps(time_config, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_custom_time_windows(metric_name):
    """
    Find metrics with custom time windows.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          window
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            window = metric["typeParams"].get("window")
            if window:
                return json.dumps({"window": window}, indent=2)
            return "No custom time window found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_cumulative_time_calculations(metric_name):
    """
    Find metrics with cumulative time-based calculations.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          window
          grainToDate
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            params = metric["typeParams"]
            if params.get("window") or params.get("grainToDate"):
                return json.dumps(params, indent=2)
            return "No cumulative time calculations found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_time_dimension_configs(metric_name):
    """
    Find metrics with specific time dimension configurations.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            time_dims = [
                {
                    "name": d["name"],
                    "granularity": d["typeParams"].get("timeGranularity")
                }
                for d in metric["dimensions"]
                if d["type"] == "TIME"
            ]
            return json.dumps(time_dims, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_time_based_filters(metric_name):
    """
    Find metrics with custom time-based filters.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        filter
        dimensions {{
          name
          type
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Check if filter contains time dimensions
            time_dims = [d["name"] for d in metric["dimensions"] if d["type"] == "TIME"]
            if metric["filter"]:
                # Check if filter references any time dimensions
                time_based = any(dim in metric["filter"] for dim in time_dims)
                if time_based:
                    return json.dumps({
                        "filter": metric["filter"],
                        "time_dimensions": time_dims
                    }, indent=2)
            return "No time-based filters found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_time_based_measure_aggregations(metric_name):
    """
    Find metrics with specific time-based measure aggregations.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          window
          grainToDate
        }}
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Extract time-based measure configuration
            time_config = {
                "measure": metric["typeParams"].get("measure"),
                "input_measures": metric["typeParams"].get("inputMeasures"),
                "window": metric["typeParams"].get("window"),
                "grain_to_date": metric["typeParams"].get("grainToDate"),
                "time_dimensions": [
                    {
                        "name": d["name"],
                        "granularity": d["typeParams"].get("timeGranularity")
                    }
                    for d in metric["dimensions"]
                    if d["type"] == "TIME"
                ]
            }
            return json.dumps(time_config, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def query_derived_metrics(metric_names, group_by=None):
    """
    Query derived metrics with complex calculations.
    
    Args:
        metric_names: List of derived metric names to query
        group_by: Optional list of dimensions to group by
    """
    # Ensure metrics is a list
    if isinstance(metric_names, str):
        metric_names = [metric_names]
    
    # Generate metric list string for GraphQL
    metric_list = ", ".join([f"{{name: \"{metric}\"}}" for metric in metric_names])
    
    # Build group_by section if needed
    group_by_section = ""
    if group_by:
        if isinstance(group_by, str):
            group_by = [group_by]
        group_by_section = f"groupBy: [{', '.join(f'{{name: \"{dim}\"}}' for dim in group_by)}]"
    
    mutation = f"""
    mutation {{
      createQuery(
        environmentId: "{CONFIG['environment_id']}"
        metrics: [{metric_list}]
        {group_by_section}
      ) {{
        queryId
      }}
    }}
    """
    
    result = execute_mutation(mutation)
    if "error" in result:
        return result["error"]
    
    # Get query ID and poll for results
    query_id = result["createQuery"]["queryId"]
    return poll_query_results(query_id)

@mcp.tool()
def analyze_ratio_metrics(metric_names, group_by=None):
    """
    Analyze ratio metrics with custom numerators and denominators.
    
    Args:
        metric_names: List of ratio metric names to analyze
        group_by: Optional list of dimensions to group by
    """
    # First get the metric details to verify they are ratio metrics
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          numerator
          denominator
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter for ratio metrics
    ratio_metrics = [
        m for m in result["metrics"]
        if m["name"] in metric_names and m["type"] == "RATIO"
    ]
    
    if not ratio_metrics:
        return "No ratio metrics found with the provided names"
    
    # Query the ratio metrics
    return query_derived_metrics(metric_names, group_by)

@mcp.tool()
def find_cumulative_metrics(metric_names, group_by=None):
    """
    Find cumulative metrics with specific window functions.
    
    Args:
        metric_names: List of metric names to analyze
        group_by: Optional list of dimensions to group by
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          window
          grainToDate
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find cumulative metrics
    cumulative_metrics = [
        m for m in result["metrics"]
        if m["name"] in metric_names and 
        (m["typeParams"].get("window") or m["typeParams"].get("grainToDate"))
    ]
    
    if not cumulative_metrics:
        return "No cumulative metrics found with the provided names"
    
    # Query the cumulative metrics
    return query_derived_metrics(metric_names, group_by)

@mcp.tool()
def find_custom_expressions(metric_name):
    """
    Find metrics with custom expressions.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          expr
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            expr = metric["typeParams"].get("expr")
            if expr:
                return json.dumps({"expression": expr}, indent=2)
            return "No custom expression found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_aggregation_types(metric_name):
    """
    Get metrics with specific aggregation types.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            params = metric["typeParams"]
            agg_config = {
                "type": metric["type"],
                "measure": params.get("measure"),
                "input_measures": params.get("inputMeasures"),
                "numerator": params.get("numerator"),
                "denominator": params.get("denominator"),
                "expression": params.get("expr")
            }
            return json.dumps(agg_config, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_custom_filter_conditions(metric_name):
    """
    Find metrics with custom filter conditions.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        filter
        dimensions {{
          name
          type
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            if metric["filter"]:
                return json.dumps({
                    "filter": metric["filter"],
                    "dimensions": metric["dimensions"]
                }, indent=2)
            return "No custom filter conditions found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_metric_type_parameters(metric_name):
    """
    Find metrics with specific metric type parameters.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            return json.dumps(metric["typeParams"], indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_custom_window_configs(metric_name):
    """
    Find metrics with custom window configurations.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          window
          grainToDate
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            params = metric["typeParams"]
            if params.get("window") or params.get("grainToDate"):
                return json.dumps(params, indent=2)
            return "No custom window configuration found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_grain_to_date_settings(metric_name):
    """
    Get metrics with specific grain-to-date settings.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          grainToDate
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            grain_to_date = metric["typeParams"].get("grainToDate")
            if grain_to_date:
                return json.dumps({"grain_to_date": grain_to_date}, indent=2)
            return "No grain-to-date settings found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def query_multi_dimensional_metrics(metric_names, dimensions, group_by=None):
    """
    Query metrics across multiple dimensions simultaneously.
    
    Args:
        metric_names: List of metric names to query
        dimensions: List of dimension names to include
        group_by: Optional list of dimensions to group by
    """
    # Ensure inputs are lists
    if isinstance(metric_names, str):
        metric_names = [metric_names]
    if isinstance(dimensions, str):
        dimensions = [dimensions]
    
    # Generate metric list string for GraphQL
    metric_list = ", ".join([f"{{name: \"{metric}\"}}" for metric in metric_names])
    
    # Build group_by section if needed
    group_by_section = ""
    if group_by:
        if isinstance(group_by, str):
            group_by = [group_by]
        group_by_section = f"groupBy: [{', '.join(f'{{name: \"{dim}\"}}' for dim in group_by)}]"
    
    mutation = f"""
    mutation {{
      createQuery(
        environmentId: "{CONFIG['environment_id']}"
        metrics: [{metric_list}]
        dimensions: [{', '.join(f'{{name: \"{dim}\"}}' for dim in dimensions)}]
        {group_by_section}
      ) {{
        queryId
      }}
    }}
    """
    
    result = execute_mutation(mutation)
    if "error" in result:
        return result["error"]
    
    # Get query ID and poll for results
    query_id = result["createQuery"]["queryId"]
    return poll_query_results(query_id)

@mcp.tool()
def analyze_categorical_time_metrics(metric_names, categorical_dims, time_dims):
    """
    Analyze metrics with categorical and time dimensions together.
    
    Args:
        metric_names: List of metric names to analyze
        categorical_dims: List of categorical dimension names
        time_dims: List of time dimension names
    """
    # Ensure inputs are lists
    if isinstance(metric_names, str):
        metric_names = [metric_names]
    if isinstance(categorical_dims, str):
        categorical_dims = [categorical_dims]
    if isinstance(time_dims, str):
        time_dims = [time_dims]
    
    # Generate metric list string for GraphQL
    metric_list = ", ".join([f"{{name: \"{metric}\"}}" for metric in metric_names])
    
    # Build dimensions section
    dimensions = []
    for dim in categorical_dims:
        dimensions.append(f'{{name: "{dim}"}}')
    for dim in time_dims:
        dimensions.append(f'{{name: "{dim}", grain: DAY}}')
    
    mutation = f"""
    mutation {{
      createQuery(
        environmentId: "{CONFIG['environment_id']}"
        metrics: [{metric_list}]
        dimensions: [{', '.join(dimensions)}]
      ) {{
        queryId
      }}
    }}
    """
    
    result = execute_mutation(mutation)
    if "error" in result:
        return result["error"]
    
    # Get query ID and poll for results
    query_id = result["createQuery"]["queryId"]
    return poll_query_results(query_id)

@mcp.tool()
def find_dimension_combinations(metric_name):
    """
    Find metrics that can be analyzed by specific dimension combinations.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Group dimensions by type
            dim_groups = {
                "categorical": [],
                "time": []
            }
            
            for dim in metric["dimensions"]:
                if dim["type"] == "TIME":
                    dim_groups["time"].append({
                        "name": dim["name"],
                        "granularity": dim["typeParams"].get("timeGranularity")
                    })
                else:
                    dim_groups["categorical"].append(dim["name"])
            
            return json.dumps(dim_groups, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_dimension_hierarchies(metric_name):
    """
    Find metrics with specific dimension hierarchies.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
            entityPath
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Extract hierarchical relationships
            hierarchies = []
            for dim in metric["dimensions"]:
                if dim["typeParams"].get("entityPath"):
                    hierarchies.append({
                        "dimension": dim["name"],
                        "type": dim["type"],
                        "entity_path": dim["typeParams"]["entityPath"]
                    })
            
            if hierarchies:
                return json.dumps(hierarchies, indent=2)
            return "No dimension hierarchies found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_custom_dimension_groups(metric_name):
    """
    Get metrics with custom dimension groupings.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
          expr
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Find dimensions with custom expressions
            custom_groups = [
                {
                    "dimension": dim["name"],
                    "type": dim["type"],
                    "expression": dim["expr"]
                }
                for dim in metric["dimensions"]
                if dim["expr"]
            ]
            
            if custom_groups:
                return json.dumps(custom_groups, indent=2)
            return "No custom dimension groupings found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_dimension_filters(metric_name):
    """
    Identify metrics with specific dimension filters.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        filter
        dimensions {{
          name
          type
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            if metric["filter"]:
                # Map filter to dimensions
                filter_map = {
                    "filter": metric["filter"],
                    "dimensions": [
                        {
                            "name": dim["name"],
                            "type": dim["type"]
                        }
                        for dim in metric["dimensions"]
                    ]
                }
                return json.dumps(filter_map, indent=2)
            return "No dimension filters found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_multi_hop_dimensions(metric_name):
    """
    Find metrics with multi-hop dimension relationships.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
          typeParams {{
            entityPath
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Find dimensions with entity paths (indicating multi-hop relationships)
            multi_hop_dims = [
                {
                    "dimension": dim["name"],
                    "type": dim["type"],
                    "entity_path": dim["typeParams"].get("entityPath")
                }
                for dim in metric["dimensions"]
                if dim["typeParams"].get("entityPath")
            ]
            
            if multi_hop_dims:
                return json.dumps(multi_hop_dims, indent=2)
            return "No multi-hop dimension relationships found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_entity_paths(metric_name):
    """
    Discover metrics with specific entity paths.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
          typeParams {{
            entityPath
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Extract entity paths
            entity_paths = [
                {
                    "dimension": dim["name"],
                    "path": dim["typeParams"].get("entityPath")
                }
                for dim in metric["dimensions"]
                if dim["typeParams"].get("entityPath")
            ]
            
            if entity_paths:
                return json.dumps(entity_paths, indent=2)
            return "No entity paths found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_custom_dimension_expressions(metric_name):
    """
    Get metrics with custom dimension expressions.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        dimensions {{
          name
          type
          expr
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Find dimensions with custom expressions
            custom_exprs = [
                {
                    "dimension": dim["name"],
                    "type": dim["type"],
                    "expression": dim["expr"]
                }
                for dim in metric["dimensions"]
                if dim["expr"]
            ]
            
            if custom_exprs:
                return json.dumps(custom_exprs, indent=2)
            return "No custom dimension expressions found"
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def list_saved_queries():
    """
    List all saved queries in the project.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
        groupBy {{
          name
        }}
        where
        orderBy {{
          name
          order
        }}
        limit
        compiledSql
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    return json.dumps(result["savedQueries"], indent=2)

@mcp.tool()
def find_frequent_query_patterns():
    """
    Find frequently used query patterns.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
        groupBy {{
          name
        }}
        where
        orderBy {{
          name
          order
        }}
        limit
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze query patterns
    patterns = {
        "metric_combinations": {},
        "dimension_combinations": {},
        "group_by_patterns": {},
        "filter_patterns": {},
        "ordering_patterns": {}
    }
    
    for query in result["savedQueries"]:
        # Analyze metric combinations
        metric_key = tuple(sorted(m["name"] for m in query["metrics"]))
        patterns["metric_combinations"][metric_key] = patterns["metric_combinations"].get(metric_key, 0) + 1
        
        # Analyze dimension combinations
        dim_key = tuple(sorted(d["name"] for d in query["dimensions"]))
        patterns["dimension_combinations"][dim_key] = patterns["dimension_combinations"].get(dim_key, 0) + 1
        
        # Analyze group by patterns
        if query["groupBy"]:
            group_key = tuple(sorted(g["name"] for g in query["groupBy"]))
            patterns["group_by_patterns"][group_key] = patterns["group_by_patterns"].get(group_key, 0) + 1
        
        # Analyze filter patterns
        if query["where"]:
            patterns["filter_patterns"][query["where"]] = patterns["filter_patterns"].get(query["where"], 0) + 1
        
        # Analyze ordering patterns
        if query["orderBy"]:
            order_key = tuple(sorted((o["name"], o["order"]) for o in query["orderBy"]))
            patterns["ordering_patterns"][order_key] = patterns["ordering_patterns"].get(order_key, 0) + 1
    
    return json.dumps(patterns, indent=2)

@mcp.tool()
def find_queries_by_context(context):
    """
    Discover queries by business context.
    
    Args:
        context: The business context to search for in query descriptions
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter queries by context in description
    filtered_queries = [
        q for q in result["savedQueries"]
        if q["description"] and context.lower() in q["description"].lower()
    ]
    return json.dumps(filtered_queries, indent=2)

@mcp.tool()
def find_queries_by_metrics(metric_names):
    """
    Get queries with specific metric combinations.
    
    Args:
        metric_names: List of metric names to search for
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter queries that contain all specified metrics
    filtered_queries = [
        q for q in result["savedQueries"]
        if all(m["name"] in [qm["name"] for qm in q["metrics"]] for m in metric_names)
    ]
    return json.dumps(filtered_queries, indent=2)

@mcp.tool()
def find_queries_with_filters():
    """
    Identify queries with custom filters and conditions.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        where
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter queries with custom filters
    filtered_queries = [
        q for q in result["savedQueries"]
        if q["where"]
    ]
    return json.dumps(filtered_queries, indent=2)

@mcp.tool()
def find_time_based_queries():
    """
    Find queries with specific time-based analysis.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        metrics {{
          name
        }}
        dimensions {{
          name
          type
        }}
        groupBy {{
          name
        }}
        where
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find queries with time dimensions or time-based filters
    time_queries = []
    for query in result["savedQueries"]:
        has_time_dim = any(d["type"] == "TIME" for d in query["dimensions"])
        has_time_filter = query["where"] and any(
            term in query["where"].lower() 
            for term in ["date", "time", "period", "day", "month", "year"]
        )
        
        if has_time_dim or has_time_filter:
            time_queries.append(query)
    
    return json.dumps(time_queries, indent=2)

@mcp.tool()
def find_queries_with_ordering():
    """
    Discover queries with specific ordering.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        orderBy {{
          name
          order
        }}
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter queries with custom ordering
    ordered_queries = [
        q for q in result["savedQueries"]
        if q["orderBy"]
    ]
    return json.dumps(ordered_queries, indent=2)

@mcp.tool()
def find_queries_with_limits():
    """
    Get queries with custom limits.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        limit
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Filter queries with custom limits
    limited_queries = [
        q for q in result["savedQueries"]
        if q["limit"]
    ]
    return json.dumps(limited_queries, indent=2)

@mcp.tool()
def find_queries_with_compilation_settings():
    """
    Find queries with specific compilation settings.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        compiledSql
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze compilation settings in queries
    compilation_patterns = {
        "complex_joins": [],
        "window_functions": [],
        "subqueries": [],
        "custom_functions": []
    }
    
    for query in result["savedQueries"]:
        sql = query["compiledSql"].lower()
        
        # Check for complex joins
        if "join" in sql and sql.count("join") > 2:
            compilation_patterns["complex_joins"].append(query)
        
        # Check for window functions
        if "over (" in sql:
            compilation_patterns["window_functions"].append(query)
        
        # Check for subqueries
        if "select" in sql and sql.count("select") > 1:
            compilation_patterns["subqueries"].append(query)
        
        # Check for custom functions
        if "function" in sql or "procedure" in sql:
            compilation_patterns["custom_functions"].append(query)
    
    return json.dumps(compilation_patterns, indent=2)

@mcp.tool()
def find_platform_compatible_metrics(platform=None):
    """
    Find metrics compatible with specific data platforms.
    
    Args:
        platform: Optional platform name to filter by (e.g., "snowflake", "bigquery")
    """
    # First get the platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    # If platform is specified, check compatibility
    if platform and platform.lower() != dialect.lower():
        return f"Warning: Specified platform '{platform}' differs from project dialect '{dialect}'"
    
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        description
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze metrics for platform compatibility
    compatible_metrics = []
    for metric in result["metrics"]:
        # Check for platform-specific features
        has_complex_features = any([
            metric["typeParams"].get("window"),
            metric["typeParams"].get("grainToDate"),
            metric["typeParams"].get("expr"),
            any(d["typeParams"].get("timeGranularity") for d in metric["dimensions"])
        ])
        
        if not has_complex_features:
            compatible_metrics.append(metric)
    
    return json.dumps(compatible_metrics, indent=2)

@mcp.tool()
def find_platform_optimizations():
    """
    Discover platform-specific query optimizations.
    """
    # Get platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        compiledSql
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze queries for platform-specific optimizations
    optimizations = {
        "dialect": dialect,
        "optimization_patterns": {
            "materialized_views": [],
            "partitioning": [],
            "clustering": [],
            "indexing": [],
            "caching": []
        }
    }
    
    for query in result["savedQueries"]:
        sql = query["compiledSql"].lower()
        
        # Check for platform-specific optimization patterns
        if dialect == "snowflake":
            if "materialized" in sql:
                optimizations["optimization_patterns"]["materialized_views"].append(query)
            if "cluster by" in sql:
                optimizations["optimization_patterns"]["clustering"].append(query)
        elif dialect == "bigquery":
            if "partition by" in sql:
                optimizations["optimization_patterns"]["partitioning"].append(query)
            if "cluster by" in sql:
                optimizations["optimization_patterns"]["clustering"].append(query)
        elif dialect == "postgres":
            if "create index" in sql:
                optimizations["optimization_patterns"]["indexing"].append(query)
            if "materialized" in sql:
                optimizations["optimization_patterns"]["materialized_views"].append(query)
    
    return json.dumps(optimizations, indent=2)

@mcp.tool()
def find_platform_aggregations():
    """
    Get metrics with platform-specific aggregations.
    """
    # Get platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze metrics for platform-specific aggregations
    platform_aggs = {
        "dialect": dialect,
        "metrics": []
    }
    
    for metric in result["metrics"]:
        params = metric["typeParams"]
        
        # Check for platform-specific aggregation patterns
        has_platform_agg = False
        agg_details = {
            "name": metric["name"],
            "type": metric["type"],
            "platform_features": []
        }
        
        if dialect == "snowflake":
            if params.get("window"):
                has_platform_agg = True
                agg_details["platform_features"].append("window_functions")
        elif dialect == "bigquery":
            if params.get("grainToDate"):
                has_platform_agg = True
                agg_details["platform_features"].append("grain_to_date")
        elif dialect == "postgres":
            if params.get("expr") and any(func in params["expr"].lower() for func in ["array_agg", "json_agg"]):
                has_platform_agg = True
                agg_details["platform_features"].append("array_aggregations")
        
        if has_platform_agg:
            platform_aggs["metrics"].append(agg_details)
    
    return json.dumps(platform_aggs, indent=2)

@mcp.tool()
def find_platform_functions():
    """
    Identify metrics with custom platform functions.
    """
    # Get platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          expr
        }}
        dimensions {{
          name
          expr
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze for platform-specific functions
    platform_funcs = {
        "dialect": dialect,
        "custom_functions": []
    }
    
    for metric in result["metrics"]:
        # Check metric expressions
        if metric["typeParams"].get("expr"):
            expr = metric["typeParams"]["expr"].lower()
            if any(func in expr for func in get_platform_function_patterns(dialect)):
                platform_funcs["custom_functions"].append({
                    "name": metric["name"],
                    "type": "metric",
                    "expression": metric["typeParams"]["expr"]
                })
        
        # Check dimension expressions
        for dim in metric["dimensions"]:
            if dim.get("expr"):
                expr = dim["expr"].lower()
                if any(func in expr for func in get_platform_function_patterns(dialect)):
                    platform_funcs["custom_functions"].append({
                        "name": dim["name"],
                        "type": "dimension",
                        "expression": dim["expr"]
                    })
    
    return json.dumps(platform_funcs, indent=2)

@mcp.tool()
def find_platform_constraints():
    """
    Find metrics with specific platform constraints.
    """
    # Get platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze for platform-specific constraints
    platform_constraints = {
        "dialect": dialect,
        "constrained_metrics": []
    }
    
    for metric in result["metrics"]:
        constraints = []
        
        # Check for platform-specific constraints
        if dialect == "snowflake":
            if metric["typeParams"].get("window"):
                constraints.append("window_function_constraints")
            if any(d["typeParams"].get("timeGranularity") for d in metric["dimensions"]):
                constraints.append("time_granularity_constraints")
        elif dialect == "bigquery":
            if metric["typeParams"].get("grainToDate"):
                constraints.append("grain_to_date_constraints")
            if metric["filter"] and "date" in metric["filter"].lower():
                constraints.append("date_filter_constraints")
        elif dialect == "postgres":
            if metric["typeParams"].get("expr") and "array" in metric["typeParams"]["expr"].lower():
                constraints.append("array_function_constraints")
        
        if constraints:
            platform_constraints["constrained_metrics"].append({
                "name": metric["name"],
                "type": metric["type"],
                "constraints": constraints
            })
    
    return json.dumps(platform_constraints, indent=2)

@mcp.tool()
def find_platform_query_patterns():
    """
    Discover platform-specific query patterns.
    """
    # Get platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        compiledSql
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze queries for platform-specific patterns
    query_patterns = {
        "dialect": dialect,
        "patterns": {
            "window_functions": [],
            "array_operations": [],
            "date_functions": [],
            "aggregation_functions": [],
            "custom_functions": []
        }
    }
    
    for query in result["savedQueries"]:
        sql = query["compiledSql"].lower()
        
        # Check for platform-specific patterns
        if dialect == "snowflake":
            if "over (" in sql:
                query_patterns["patterns"]["window_functions"].append(query)
            if any(func in sql for func in ["array_agg", "array_construct"]):
                query_patterns["patterns"]["array_operations"].append(query)
        elif dialect == "bigquery":
            if any(func in sql for func in ["date_trunc", "date_add", "date_sub"]):
                query_patterns["patterns"]["date_functions"].append(query)
            if any(func in sql for func in ["array_agg", "array_length"]):
                query_patterns["patterns"]["array_operations"].append(query)
        elif dialect == "postgres":
            if any(func in sql for func in ["generate_series", "date_trunc"]):
                query_patterns["patterns"]["date_functions"].append(query)
            if any(func in sql for func in ["array_agg", "array_length"]):
                query_patterns["patterns"]["array_operations"].append(query)
        
        # Check for custom functions
        if "function" in sql or "procedure" in sql:
            query_patterns["patterns"]["custom_functions"].append(query)
    
    return json.dumps(query_patterns, indent=2)

@mcp.tool()
def find_platform_configurations():
    """
    Get metrics with specific platform configurations.
    """
    # Get platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze for platform-specific configurations
    platform_configs = {
        "dialect": dialect,
        "configured_metrics": []
    }
    
    for metric in result["metrics"]:
        configs = []
        
        # Check for platform-specific configurations
        if dialect == "snowflake":
            if metric["typeParams"].get("window"):
                configs.append("window_function_config")
            if any(d["typeParams"].get("timeGranularity") for d in metric["dimensions"]):
                configs.append("time_granularity_config")
        elif dialect == "bigquery":
            if metric["typeParams"].get("grainToDate"):
                configs.append("grain_to_date_config")
            if metric["filter"] and "date" in metric["filter"].lower():
                configs.append("date_filter_config")
        elif dialect == "postgres":
            if metric["typeParams"].get("expr") and "array" in metric["typeParams"]["expr"].lower():
                configs.append("array_function_config")
        
        if configs:
            platform_configs["configured_metrics"].append({
                "name": metric["name"],
                "type": metric["type"],
                "configurations": configs
            })
    
    return json.dumps(platform_configs, indent=2)

@mcp.tool()
def find_platform_expressions():
    """
    Find metrics with custom platform expressions.
    """
    # Get platform dialect
    dialect_result = get_platform_dialect()
    if isinstance(dialect_result, str):
        return dialect_result
    
    dialect = json.loads(dialect_result)["dialect"]
    
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          expr
        }}
        dimensions {{
          name
          expr
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze for platform-specific expressions
    platform_exprs = {
        "dialect": dialect,
        "custom_expressions": []
    }
    
    for metric in result["metrics"]:
        # Check metric expressions
        if metric["typeParams"].get("expr"):
            expr = metric["typeParams"]["expr"].lower()
            if any(func in expr for func in get_platform_function_patterns(dialect)):
                platform_exprs["custom_expressions"].append({
                    "name": metric["name"],
                    "type": "metric",
                    "expression": metric["typeParams"]["expr"]
                })
        
        # Check dimension expressions
        for dim in metric["dimensions"]:
            if dim.get("expr"):
                expr = dim["expr"].lower()
                if any(func in expr for func in get_platform_function_patterns(dialect)):
                    platform_exprs["custom_expressions"].append({
                        "name": dim["name"],
                        "type": "dimension",
                        "expression": dim["expr"]
                    })
    
    return json.dumps(platform_exprs, indent=2)

def get_platform_function_patterns(dialect):
    """Helper function to get platform-specific function patterns"""
    patterns = {
        "snowflake": [
            "array_agg", "array_construct", "date_trunc", "dateadd", "datediff",
            "window", "over", "partition by", "order by"
        ],
        "bigquery": [
            "array_agg", "array_length", "date_trunc", "date_add", "date_sub",
            "timestamp_trunc", "timestamp_add", "timestamp_sub"
        ],
        "postgres": [
            "array_agg", "array_length", "date_trunc", "generate_series",
            "json_agg", "jsonb_agg", "string_agg"
        ]
    }
    return patterns.get(dialect.lower(), [])

@mcp.tool()
def poll_query_results(query_id, max_attempts=30):
    """Poll for query results until they're ready or timeout is reached"""
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        
        query = f"""{{
          query(environmentId: "{CONFIG['environment_id']}", queryId: "{query_id}") {{
            status
            error
            jsonResult(encoded: false)
          }}
        }}"""
        
        result = execute_query(query)
        if "error" in result:
            return f"Error polling results: {result['error']}"
        
        query_result = result["query"]
        
        if query_result["status"] == "FAILED":
            return f"Query failed: {query_result['error']}"
        elif query_result["status"] == "SUCCESSFUL":
            return query_result["jsonResult"]
        
        time.sleep(1)
    
    return "Query timed out. Please try again or simplify your query."

@mcp.tool()
def analyze_query_patterns():
    """
    Analyze query patterns for optimization opportunities.
    """
    query = f"""{{
      savedQueries(environmentId: "{CONFIG['environment_id']}") {{
        id
        name
        description
        compiledSql
        metrics {{
          name
        }}
        dimensions {{
          name
        }}
        groupBy {{
          name
        }}
        where
        orderBy {{
          name
          order
        }}
        limit
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze query patterns for optimization opportunities
    patterns = {
        "complex_joins": [],
        "subqueries": [],
        "window_functions": [],
        "aggregations": [],
        "filters": [],
        "ordering": [],
        "limits": []
    }
    
    for query in result["savedQueries"]:
        sql = query["compiledSql"].lower()
        
        # Check for complex joins
        if "join" in sql and sql.count("join") > 2:
            patterns["complex_joins"].append({
                "query": query["name"],
                "join_count": sql.count("join")
            })
        
        # Check for subqueries
        if "select" in sql and sql.count("select") > 1:
            patterns["subqueries"].append({
                "query": query["name"],
                "subquery_count": sql.count("select") - 1
            })
        
        # Check for window functions
        if "over (" in sql:
            patterns["window_functions"].append({
                "query": query["name"],
                "window_count": sql.count("over (")
            })
        
        # Check for aggregations
        agg_functions = ["sum(", "count(", "avg(", "min(", "max(", "group by"]
        if any(func in sql for func in agg_functions):
            patterns["aggregations"].append({
                "query": query["name"],
                "aggregation_types": [func for func in agg_functions if func in sql]
            })
        
        # Check for filters
        if query["where"]:
            patterns["filters"].append({
                "query": query["name"],
                "filter": query["where"]
            })
        
        # Check for ordering
        if query["orderBy"]:
            patterns["ordering"].append({
                "query": query["name"],
                "order_by": query["orderBy"]
            })
        
        # Check for limits
        if query["limit"]:
            patterns["limits"].append({
                "query": query["name"],
                "limit": query["limit"]
            })
    
    return json.dumps(patterns, indent=2)

@mcp.tool()
def find_complex_metrics():
    """
    Find metrics with high query complexity.
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze metrics for complexity
    complex_metrics = []
    for metric in result["metrics"]:
        complexity_score = 0
        complexity_factors = []
        
        # Check metric type complexity
        if metric["type"] in ["RATIO", "DERIVED"]:
            complexity_score += 2
            complexity_factors.append("complex_metric_type")
        
        # Check for complex expressions
        if metric["typeParams"].get("expr"):
            complexity_score += 1
            complexity_factors.append("custom_expression")
        
        # Check for window functions
        if metric["typeParams"].get("window"):
            complexity_score += 2
            complexity_factors.append("window_function")
        
        # Check for grain-to-date calculations
        if metric["typeParams"].get("grainToDate"):
            complexity_score += 1
            complexity_factors.append("grain_to_date")
        
        # Check for multiple input measures
        if metric["typeParams"].get("inputMeasures") and len(metric["typeParams"]["inputMeasures"]) > 1:
            complexity_score += 1
            complexity_factors.append("multiple_input_measures")
        
        # Check for complex filters
        if metric["filter"]:
            complexity_score += 1
            complexity_factors.append("custom_filter")
        
        # Check for time-based dimensions
        if any(d["type"] == "TIME" for d in metric["dimensions"]):
            complexity_score += 1
            complexity_factors.append("time_dimensions")
        
        # Consider metric complex if score is high
        if complexity_score >= 3:
            complex_metrics.append({
                "name": metric["name"],
                "type": metric["type"],
                "complexity_score": complexity_score,
                "complexity_factors": complexity_factors
            })
    
    return json.dumps(complex_metrics, indent=2)

@mcp.tool()
def find_cacheable_metrics():
    """
    Discover metrics that could benefit from caching.
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze metrics for caching potential
    cacheable_metrics = []
    for metric in result["metrics"]:
        caching_score = 0
        caching_factors = []
        
        # Check for simple metric types
        if metric["type"] == "SIMPLE":
            caching_score += 2
            caching_factors.append("simple_metric_type")
        
        # Check for no custom expressions
        if not metric["typeParams"].get("expr"):
            caching_score += 1
            caching_factors.append("no_custom_expressions")
        
        # Check for no window functions
        if not metric["typeParams"].get("window"):
            caching_score += 1
            caching_factors.append("no_window_functions")
        
        # Check for no grain-to-date calculations
        if not metric["typeParams"].get("grainToDate"):
            caching_score += 1
            caching_factors.append("no_grain_to_date")
        
        # Check for simple filters
        if not metric["filter"] or metric["filter"].lower() in ["true", "1=1"]:
            caching_score += 1
            caching_factors.append("simple_filters")
        
        # Check for limited dimensions
        if len(metric["dimensions"]) <= 3:
            caching_score += 1
            caching_factors.append("limited_dimensions")
        
        # Consider metric cacheable if score is high
        if caching_score >= 5:
            cacheable_metrics.append({
                "name": metric["name"],
                "type": metric["type"],
                "caching_score": caching_score,
                "caching_factors": caching_factors
            })
    
    return json.dumps(cacheable_metrics, indent=2)

@mcp.tool()
def get_performance_characteristics(metric_name):
    """
    Get metrics with specific performance characteristics.
    
    Args:
        metric_name: The name of the metric to analyze
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Find the specific metric
    for metric in result["metrics"]:
        if metric["name"] == metric_name:
            # Analyze performance characteristics
            characteristics = {
                "name": metric["name"],
                "type": metric["type"],
                "performance_factors": {
                    "query_complexity": {
                        "score": 0,
                        "factors": []
                    },
                    "resource_usage": {
                        "score": 0,
                        "factors": []
                    },
                    "caching_potential": {
                        "score": 0,
                        "factors": []
                    }
                }
            }
            
            # Analyze query complexity
            if metric["type"] in ["RATIO", "DERIVED"]:
                characteristics["performance_factors"]["query_complexity"]["score"] += 2
                characteristics["performance_factors"]["query_complexity"]["factors"].append("complex_metric_type")
            
            if metric["typeParams"].get("expr"):
                characteristics["performance_factors"]["query_complexity"]["score"] += 1
                characteristics["performance_factors"]["query_complexity"]["factors"].append("custom_expression")
            
            if metric["typeParams"].get("window"):
                characteristics["performance_factors"]["query_complexity"]["score"] += 2
                characteristics["performance_factors"]["query_complexity"]["factors"].append("window_function")
            
            # Analyze resource usage
            if metric["typeParams"].get("grainToDate"):
                characteristics["performance_factors"]["resource_usage"]["score"] += 1
                characteristics["performance_factors"]["resource_usage"]["factors"].append("grain_to_date")
            
            if metric["typeParams"].get("inputMeasures") and len(metric["typeParams"]["inputMeasures"]) > 1:
                characteristics["performance_factors"]["resource_usage"]["score"] += 1
                characteristics["performance_factors"]["resource_usage"]["factors"].append("multiple_input_measures")
            
            # Analyze caching potential
            if metric["type"] == "SIMPLE":
                characteristics["performance_factors"]["caching_potential"]["score"] += 2
                characteristics["performance_factors"]["caching_potential"]["factors"].append("simple_metric_type")
            
            if not metric["typeParams"].get("expr"):
                characteristics["performance_factors"]["caching_potential"]["score"] += 1
                characteristics["performance_factors"]["caching_potential"]["factors"].append("no_custom_expressions")
            
            if not metric["filter"] or metric["filter"].lower() in ["true", "1=1"]:
                characteristics["performance_factors"]["caching_potential"]["score"] += 1
                characteristics["performance_factors"]["caching_potential"]["factors"].append("simple_filters")
            
            return json.dumps(characteristics, indent=2)
    
    return f"Metric '{metric_name}' not found"

@mcp.tool()
def find_query_hints():
    """
    Identify metrics with custom query hints.
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze for query hints
    query_hints = []
    for metric in result["metrics"]:
        hints = []
        
        # Check for window function hints
        if metric["typeParams"].get("window"):
            hints.append({
                "type": "window_function",
                "hint": metric["typeParams"]["window"]
            })
        
        # Check for filter hints
        if metric["filter"]:
            hints.append({
                "type": "filter",
                "hint": metric["filter"]
            })
        
        # Check for time dimension hints
        for dim in metric["dimensions"]:
            if dim["type"] == "TIME" and dim["typeParams"].get("timeGranularity"):
                hints.append({
                    "type": "time_granularity",
                    "dimension": dim["name"],
                    "hint": dim["typeParams"]["timeGranularity"]
                })
        
        if hints:
            query_hints.append({
                "name": metric["name"],
                "type": metric["type"],
                "hints": hints
            })
    
    return json.dumps(query_hints, indent=2)

@mcp.tool()
def find_resource_requirements():
    """
    Find metrics with specific resource requirements.
    """
    query = f"""{{
      metrics(environmentId: "{CONFIG['environment_id']}") {{
        name
        type
        typeParams {{
          measure
          inputMeasures
          numerator
          denominator
          expr
          window
          grainToDate
          metrics
        }}
        filter
        dimensions {{
          name
          type
          typeParams {{
            timeGranularity
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if "error" in result:
        return result["error"]
    
    # Analyze for resource requirements
    resource_requirements = []
    for metric in result["metrics"]:
        requirements = {
            "name": metric["name"],
            "type": metric["type"],
            "requirements": {
                "memory": 0,
                "processing": 0,
                "factors": []
            }
        }
        
        # Check for memory-intensive operations
        if metric["typeParams"].get("window"):
            requirements["requirements"]["memory"] += 2
            requirements["requirements"]["factors"].append("window_functions")
        
        if metric["typeParams"].get("grainToDate"):
            requirements["requirements"]["memory"] += 1
            requirements["requirements"]["factors"].append("grain_to_date")

@mcp.tool()
def find_metrics_by_access_pattern(access_pattern=None):
    """
    Find metrics with specific access patterns
    
    Args:
        access_pattern: Optional access pattern to filter by (e.g., 'read_only', 'write', 'admin')
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            accessPatterns {{
              pattern
              description
              roles
              permissions
              restrictions
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        if access_pattern:
            metrics = [
                metric for metric in metrics
                if any(
                    pattern["pattern"] == access_pattern
                    for pattern in metric.get("accessPatterns", [])
                )
            ]
        
        return json.dumps(metrics, indent=2)
    except Exception as e:
        return f"Error finding metrics by access pattern: {str(e)}"

@mcp.tool()
def find_sensitive_metrics():
    """
    Find metrics that contain sensitive data
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            sensitivity {{
              level
              description
              dataTypes
              restrictions
              masking
              encryption
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        sensitive_metrics = [
            metric for metric in metrics
            if metric.get("sensitivity", {}).get("level") in ["high", "critical"]
        ]
        
        return json.dumps(sensitive_metrics, indent=2)
    except Exception as e:
        return f"Error finding sensitive metrics: {str(e)}"

@mcp.tool()
def find_metrics_with_security_rules():
    """
    Find metrics with custom security rules
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            securityRules {{
              rule
              description
              conditions
              actions
              priority
              scope
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        metrics_with_rules = [
            metric for metric in metrics
            if metric.get("securityRules")
        ]
        
        return json.dumps(metrics_with_rules, indent=2)
    except Exception as e:
        return f"Error finding metrics with security rules: {str(e)}"

@mcp.tool()
def find_metrics_by_permission(permission):
    """
    Find metrics with specific user permissions
    
    Args:
        permission: The permission to filter by (e.g., 'read', 'write', 'admin')
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            permissions {{
              role
              permission
              conditions
              scope
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        metrics_with_permission = [
            metric for metric in metrics
            if any(
                perm["permission"] == permission
                for perm in metric.get("permissions", [])
            )
        ]
        
        return json.dumps(metrics_with_permission, indent=2)
    except Exception as e:
        return f"Error finding metrics by permission: {str(e)}"

@mcp.tool()
def find_metrics_with_rls():
    """
    Find metrics with custom row-level security
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            rowLevelSecurity {{
              enabled
              policies
              conditions
              roles
              scope
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        metrics_with_rls = [
            metric for metric in metrics
            if metric.get("rowLevelSecurity", {}).get("enabled")
        ]
        
        return json.dumps(metrics_with_rls, indent=2)
    except Exception as e:
        return f"Error finding metrics with RLS: {str(e)}"

@mcp.tool()
def find_metrics_with_data_masking():
    """
    Find metrics with specific data masking requirements
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            dataMasking {{
              enabled
              type
              rules
              conditions
              roles
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        metrics_with_masking = [
            metric for metric in metrics
            if metric.get("dataMasking", {}).get("enabled")
        ]
        
        return json.dumps(metrics_with_masking, indent=2)
    except Exception as e:
        return f"Error finding metrics with data masking: {str(e)}"

@mcp.tool()
def find_metrics_by_auth_requirement(auth_type=None):
    """
    Find metrics with specific authentication requirements
    
    Args:
        auth_type: Optional authentication type to filter by (e.g., 'basic', 'oauth', 'sso')
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            authentication {{
              type
              requirements
              roles
              conditions
              scope
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        if auth_type:
            metrics = [
                metric for metric in metrics
                if metric.get("authentication", {}).get("type") == auth_type
            ]
        
        return json.dumps(metrics, indent=2)
    except Exception as e:
        return f"Error finding metrics by auth requirement: {str(e)}"

@mcp.tool()
def find_metrics_with_access_controls():
    """
    Find metrics with custom access controls
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            accessControls {{
              type
              rules
              conditions
              roles
              scope
              priority
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        metrics_with_controls = [
            metric for metric in metrics
            if metric.get("accessControls")
        ]
        
        return json.dumps(metrics_with_controls, indent=2)
    except Exception as e:
        return f"Error finding metrics with access controls: {str(e)}"

@mcp.tool()
def find_metrics_by_security_policy(policy_type=None):
    """
    Find metrics with specific security policies
    
    Args:
        policy_type: Optional security policy type to filter by (e.g., 'data_protection', 'access_control', 'audit')
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          metrics(environmentId: "{CONFIG['environment_id']}") {{
            name
            description
            type
            securityPolicies {{
              type
              description
              rules
              conditions
              roles
              scope
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        metrics = data["data"]["metrics"]
        if policy_type:
            metrics = [
                metric for metric in metrics
                if any(
                    policy["type"] == policy_type
                    for policy in metric.get("securityPolicies", [])
                )
            ]
        
        return json.dumps(metrics, indent=2)
    except Exception as e:
        return f"Error finding metrics by security policy: {str(e)}"

@mcp.tool()
def execute_metric_query(metrics, dimensions=None, filters=None, time_grain=None, limit=None, offset=None):
    """
    Execute a query with specific parameters
    
    Args:
        metrics: List of metric names or a single metric name
        dimensions: Optional list of dimensions to include
        filters: Optional list of filter conditions
        time_grain: Optional time grain (DAY, WEEK, MONTH, QUARTER, YEAR)
        limit: Optional limit for number of results
        offset: Optional offset for pagination
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        # Ensure metrics is a list
        if isinstance(metrics, str):
            metrics = [metrics]
            
        # Generate metric list string for GraphQL
        metric_list = ", ".join([f"{{name: \"{metric}\"}}" for metric in metrics])
        
        # Build dimensions section if needed
        dimensions_section = ""
        if dimensions:
            dims = []
            for dim in dimensions:
                if dim == "metric_time" and time_grain:
                    dims.append(f'{{name: "{dim}", grain: {time_grain}}}')
                else:
                    dims.append(f'{{name: "{dim}"}}')
            dimensions_section = f"dimensions: [{', '.join(dims)}]"
        
        # Build filters section if needed
        filters_section = ""
        if filters:
            filter_list = ", ".join([f"{{condition: \"{f}\"}}" for f in filters])
            filters_section = f"filters: [{filter_list}]"
        
        # Build pagination section if needed
        pagination_section = ""
        if limit is not None or offset is not None:
            parts = []
            if limit is not None:
                parts.append(f"limit: {limit}")
            if offset is not None:
                parts.append(f"offset: {offset}")
            pagination_section = ", ".join(parts)
        
        # Build create query mutation
        mutation = f"""
        mutation {{
          createQuery(
            environmentId: "{CONFIG['environment_id']}"
            metrics: [{metric_list}]
            {dimensions_section}
            {filters_section}
            {pagination_section}
          ) {{
            queryId
          }}
        }}
        """
        
        # Execute query and get results
        query_id = execute_mutation(mutation)["data"]["createQuery"]["queryId"]
        return poll_query_results(query_id)
        
    except Exception as e:
        return f"Error executing query: {str(e)}"

@mcp.tool()
def get_query_results(query_id, format="json"):
    """
    Get query results in various formats (Arrow, JSON)
    
    Args:
        query_id: The ID of the query to get results for
        format: The desired output format ("json" or "arrow")
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        # Build result field based on format
        result_field = "arrowResult" if format == "arrow" else "jsonResult(encoded: false)"
        
        query = f"""
        {{
          query(environmentId: "{CONFIG['environment_id']}", queryId: "{query_id}") {{
            status
            error
            {result_field}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        query_result = data["data"]["query"]
        if query_result["status"] == "FAILED":
            return f"Query failed: {query_result['error']}"
        
        return query_result[result_field]
    except Exception as e:
        return f"Error getting query results: {str(e)}"

@mcp.tool()
def find_execution_patterns():
    """
    Find queries with specific execution patterns
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          queryExecutionPatterns(environmentId: "{CONFIG['environment_id']}") {{
            pattern
            description
            frequency
            performance
            complexity
            metrics
            dimensions
            filters
            timeGrain
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        return json.dumps(data["data"]["queryExecutionPatterns"], indent=2)
    except Exception as e:
        return f"Error finding execution patterns: {str(e)}"

@mcp.tool()
def find_custom_result_formats():
    """
    Discover queries with custom result formats
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          queryResultFormats(environmentId: "{CONFIG['environment_id']}") {{
            format
            description
            encoding
            compression
            schema
            validation
            transformation
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        return json.dumps(data["data"]["queryResultFormats"], indent=2)
    except Exception as e:
        return f"Error finding custom result formats: {str(e)}"

@mcp.tool()
def get_pagination_settings(query_id):
    """
    Get queries with specific pagination settings
    
    Args:
        query_id: The ID of the query to get pagination settings for
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          query(environmentId: "{CONFIG['environment_id']}", queryId: "{query_id}") {{
            pagination {{
              type
              pageSize
              offset
              total
              hasMore
              nextOffset
            }}
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        return json.dumps(data["data"]["query"]["pagination"], indent=2)
    except Exception as e:
        return f"Error getting pagination settings: {str(e)}"

@mcp.tool()
def find_result_processing_rules():
    """
    Identify queries with custom result processing
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          queryResultProcessing(environmentId: "{CONFIG['environment_id']}") {{
            rule
            description
            condition
            action
            priority
            scope
            metrics
            dimensions
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        return json.dumps(data["data"]["queryResultProcessing"], indent=2)
    except Exception as e:
        return f"Error finding result processing rules: {str(e)}"

@mcp.tool()
def find_error_handling_rules():
    """
    Find queries with specific error handling
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          queryErrorHandling(environmentId: "{CONFIG['environment_id']}") {{
            rule
            description
            errorType
            condition
            action
            retryPolicy
            fallback
            notification
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        return json.dumps(data["data"]["queryErrorHandling"], indent=2)
    except Exception as e:
        return f"Error finding error handling rules: {str(e)}"

@mcp.tool()
def find_result_transformations():
    """
    Discover queries with custom result transformations
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          queryResultTransformations(environmentId: "{CONFIG['environment_id']}") {{
            transformation
            description
            type
            rules
            conditions
            metrics
            dimensions
            output
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        return json.dumps(data["data"]["queryResultTransformations"], indent=2)
    except Exception as e:
        return f"Error finding result transformations: {str(e)}"

@mcp.tool()
def find_result_validation_rules():
    """
    Get queries with specific result validation
    """
    if not CONFIG["is_connected"]:
        if auto_connect():
            print("Auto-connected to the semantic layer")
        else:
            return "Not connected. Use connect() first."
    
    try:
        url = f"https://{CONFIG['host']}/api/graphql"
        headers = {"Authorization": f"Bearer {CONFIG['token']}"}
        
        query = f"""
        {{
          queryResultValidation(environmentId: "{CONFIG['environment_id']}") {{
            rule
            description
            type
            conditions
            metrics
            dimensions
            thresholds
            actions
          }}
        }}
        """
        
        response = requests.post(
            url, 
            headers=headers,
            json={"query": query}
        )
        
        data = response.json()
        if "errors" in data:
            return f"GraphQL error: {data['errors']}"
        
        return json.dumps(data["data"]["queryResultValidation"], indent=2)
    except Exception as e:
        return f"Error finding result validation rules: {str(e)}"

if __name__ == "__main__":
    # Check environment variables and connection status
    missing_vars = check_required_env_vars()
    if missing_vars:
        print("\n⚠️ Missing required environment variables:")
        print(f"   {', '.join(missing_vars)}")
        print("\nPlease set these environment variables before starting the server.")
        print("You can set them using:")
        for var in missing_vars:
            print(f"export {var}=<your-value>")
        print()
    
    # Try to connect if auto-connect is enabled
    if AUTO_CONNECT and not CONFIG["is_connected"]:
        auto_connect()
    
    # Show connection status
    if CONFIG["is_connected"]:
        print("\n✅ Ready to use! The semantic layer connection is active.")
        print("Available tools: Use 'help()' to see all available tools\n")
    else:
        print("\n⚠️ Not connected to the semantic layer.")
        print("Make sure environment variables are set and use 'connect()' to establish a connection.\n")
    
    mcp.run() 