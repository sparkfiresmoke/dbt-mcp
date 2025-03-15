#!/usr/bin/env python
"""
MCP Server for dbt Discovery API - Model Lineage and Documentation Functions

This module contains functions for analyzing model lineage and documentation.
"""

import json
import time
from mcp.server.fastmcp import FastMCP
from dbt_api_utils import CONFIG, execute_query, format_json_response

# Create the MCP server
mcp = FastMCP("dbt Discovery API - Lineage and Documentation")

@mcp.tool()
def find_model_dependencies(model_name):
    """
    Get upstream and downstream dependencies for a specific model
    
    Args:
        model_name: The name of the model to analyze
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = f"""{{
      model(name: "{model_name}") {{
        name
        description
        upstreamModels {{
          name
          description
          type
        }}
        downstreamModels {{
          name
          description
          type
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    return json.dumps(result, indent=2)

@mcp.tool()
def find_affected_models(model_name):
    """
    Identify which models would be affected by changes to a specific model
    
    Args:
        model_name: The name of the model to analyze
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = f"""{{
      model(name: "{model_name}") {{
        name
        description
        downstreamModels {{
          name
          description
          type
          downstreamModels {{
            name
            description
            type
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    return json.dumps(result, indent=2)

@mcp.tool()
def get_model_lineage(model_name):
    """
    Get a complete lineage graph for any model in the project
    
    Args:
        model_name: The name of the model to analyze
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = f"""{{
      model(name: "{model_name}") {{
        name
        description
        upstreamModels {{
          name
          description
          type
          upstreamModels {{
            name
            description
            type
          }}
        }}
        downstreamModels {{
          name
          description
          type
          downstreamModels {{
            name
            description
            type
          }}
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    return json.dumps(result, indent=2)

@mcp.tool()
def find_critical_business_models():
    """
    Identify models that are part of critical business processes
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        tags
        meta
        downstreamModels {
          name
          description
          type
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with critical tags or high downstream impact
    critical_models = []
    for model in result.get("models", []):
        if any(tag in ["critical", "business_critical", "key_process"] for tag in model.get("tags", [])):
            critical_models.append(model)
        elif len(model.get("downstreamModels", [])) > 5:  # High downstream impact
            critical_models.append(model)
            
    return json.dumps({"critical_models": critical_models}, indent=2)

@mcp.tool()
def find_multi_use_models():
    """
    Find models that are used in multiple downstream applications
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        downstreamModels {
          name
          description
          type
          meta
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with multiple downstream applications
    multi_app_models = []
    for model in result.get("models", []):
        downstream_apps = set()
        for downstream in model.get("downstreamModels", []):
            if downstream.get("meta", {}).get("application"):
                downstream_apps.add(downstream["meta"]["application"])
        
        if len(downstream_apps) > 1:
            model["downstream_applications"] = list(downstream_apps)
            multi_app_models.append(model)
            
    return json.dumps({"multi_application_models": multi_app_models}, indent=2)

@mcp.tool()
def get_model_documentation(model_name):
    """
    Get comprehensive documentation for a specific model
    
    Args:
        model_name: The name of the model to analyze
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = f"""{{
      model(name: "{model_name}") {{
        name
        description
        type
        schema
        columns {{
          name
          description
          type
          tests {{
            name
            description
          }}
        }}
        tags
        meta
        docs {{
          show
          nodeColor
        }}
      }}
    }}"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    return json.dumps(result, indent=2)

@mcp.tool()
def find_models_with_missing_docs():
    """
    Find models with missing or incomplete documentation
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        columns {
          name
          description
        }
        docs {
          show
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with missing documentation
    missing_docs = []
    for model in result.get("models", []):
        missing = False
        if not model.get("description"):
            missing = True
        for column in model.get("columns", []):
            if not column.get("description"):
                missing = True
        if missing:
            missing_docs.append(model)
            
    return json.dumps({"models_with_missing_docs": missing_docs}, indent=2)

@mcp.tool()
def find_models_by_metadata(tags=None, owner=None):
    """
    Get a list of all models with specific tags or owners
    
    Args:
        tags: Optional list of tags to filter by
        owner: Optional owner to filter by
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        tags
        meta
        owner
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter by tags and/or owner if provided
    filtered_models = []
    for model in result.get("models", []):
        if tags and not any(tag in model.get("tags", []) for tag in tags):
            continue
        if owner and model.get("owner") != owner and model.get("meta", {}).get("owner") != owner:
            continue
        filtered_models.append(model)
        
    return json.dumps({"models": filtered_models}, indent=2)

@mcp.tool()
def find_stale_models(days_threshold=30):
    """
    Find models that haven't been updated recently
    
    Args:
        days_threshold: Number of days to consider a model outdated
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        meta
        lastModified
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for outdated models
    outdated_models = []
    current_time = time.time()
    threshold_seconds = days_threshold * 24 * 60 * 60
    
    for model in result.get("models", []):
        last_modified = model.get("lastModified")
        if last_modified and (current_time - last_modified) > threshold_seconds:
            model["days_since_modified"] = int((current_time - last_modified) / (24 * 60 * 60))
            outdated_models.append(model)
            
    return json.dumps({"outdated_models": outdated_models}, indent=2)

@mcp.tool()
def find_models_by_domain():
    """
    Get a list of models with specific business domains or categories
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Group models by domain
    domain_models = {}
    for model in result.get("models", []):
        # Extract domain from tags or metadata
        domain = None
        for tag in model.get("tags", []):
            if tag.startswith(("domain.", "business_unit.", "department.")):
                domain = tag.split(".")[1]
                break
                
        if not domain and model.get("meta", {}).get("domain"):
            domain = model["meta"]["domain"]
            
        if not domain:
            domain = model.get("schema", "unknown")
            
        if domain not in domain_models:
            domain_models[domain] = []
        domain_models[domain].append(model)
        
    return json.dumps({"models_by_domain": domain_models}, indent=2)

if __name__ == "__main__":
    if not CONFIG["is_connected"]:
        print("\n⚠️ Not connected to the discovery API.")
        print("Make sure environment variables are set and use 'connect()' to establish a connection.\n")
    
    mcp.run() 