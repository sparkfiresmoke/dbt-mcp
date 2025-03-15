#!/usr/bin/env python
"""
MCP Server for dbt Discovery API - Source and Package Management Functions

This module contains functions for analyzing source data and package dependencies.
"""

import json
from mcp.server.fastmcp import FastMCP
from discovery_api_server import CONFIG, execute_query

# Create the MCP server
mcp = FastMCP("dbt Discovery API - Source and Package Management")

@mcp.tool()
def find_models_by_source_quality():
    """
    Find models with source data quality issues
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        upstreamModels {
          name
          type
        }
        sources {
          name
          description
          freshness {
            status
            lastModified
          }
        }
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with source quality issues
    source_issues = []
    for model in result.get("models", []):
        issues = []
        
        # Check source freshness
        for source in model.get("sources", []):
            freshness = source.get("freshness", {})
            if freshness.get("status") == "error":
                issues.append(f"Source {source['name']} has freshness issues")
                
        # Check for source-related tags
        source_tags = ["source_issue", "data_quality", "needs_attention"]
        if any(tag in model.get("tags", []) for tag in source_tags):
            issues.append("Marked as having source issues")
            
        if issues:
            model["source_issues"] = issues
            source_issues.append(model)
            
    return json.dumps({"models_with_source_issues": source_issues}, indent=2)

@mcp.tool()
def find_models_by_source_dependencies():
    """
    Find models with complex source dependencies
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        sources {
          name
          description
          type
          freshness {
            status
            lastModified
          }
        }
        upstreamModels {
          name
          type
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Analyze source dependencies
    dependency_data = []
    for model in result.get("models", []):
        dep_info = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "sources": model.get("sources", []),
            "upstream_models": model.get("upstreamModels", []),
            "dependency_complexity": 0
        }
        
        # Calculate dependency complexity
        source_count = len(model.get("sources", []))
        upstream_count = len(model.get("upstreamModels", []))
        
        if source_count > 3:
            dep_info["dependency_complexity"] += 2
        if upstream_count > 3:
            dep_info["dependency_complexity"] += 2
            
        dependency_data.append(dep_info)
        
    return json.dumps({"source_dependency_analysis": dependency_data}, indent=2)

@mcp.tool()
def find_models_by_package_dependencies():
    """
    Find models with external package dependencies
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        packageName
        dependencies {
          name
          version
          type
        }
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with package dependencies
    package_deps = []
    for model in result.get("models", []):
        deps = model.get("dependencies", [])
        if deps:
            dep_info = {
                "model_name": model["name"],
                "description": model.get("description"),
                "type": model.get("type"),
                "package_name": model.get("packageName"),
                "dependencies": deps,
                "external_dependencies": [d for d in deps if d.get("type") == "external"]
            }
            package_deps.append(dep_info)
            
    return json.dumps({"models_with_package_dependencies": package_deps}, indent=2)

@mcp.tool()
def find_models_by_package_version():
    """
    Find models using outdated package versions
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        packageName
        dependencies {
          name
          version
          type
          latestVersion
        }
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with outdated packages
    outdated_packages = []
    for model in result.get("models", []):
        outdated = []
        for dep in model.get("dependencies", []):
            if dep.get("type") == "external" and dep.get("version") != dep.get("latestVersion"):
                outdated.append({
                    "package": dep["name"],
                    "current_version": dep["version"],
                    "latest_version": dep["latestVersion"]
                })
                
        if outdated:
            model["outdated_packages"] = outdated
            outdated_packages.append(model)
            
    return json.dumps({"models_with_outdated_packages": outdated_packages}, indent=2)

@mcp.tool()
def find_models_by_source_freshness():
    """
    Find models with stale source data
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        sources {
          name
          description
          freshness {
            status
            lastModified
            maxAge
          }
        }
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with stale sources
    stale_sources = []
    for model in result.get("models", []):
        stale = []
        for source in model.get("sources", []):
            freshness = source.get("freshness", {})
            if freshness.get("status") == "error" or freshness.get("status") == "warn":
                stale.append({
                    "source": source["name"],
                    "status": freshness.get("status"),
                    "last_modified": freshness.get("lastModified"),
                    "max_age": freshness.get("maxAge")
                })
                
        if stale:
            model["stale_sources"] = stale
            stale_sources.append(model)
            
    return json.dumps({"models_with_stale_sources": stale_sources}, indent=2)

@mcp.tool()
def find_models_by_package_health():
    """
    Find models using packages with health issues
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        packageName
        dependencies {
          name
          version
          type
          health {
            status
            issues
            lastChecked
          }
        }
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with unhealthy packages
    unhealthy_packages = []
    for model in result.get("models", []):
        unhealthy = []
        for dep in model.get("dependencies", []):
            health = dep.get("health", {})
            if health.get("status") == "error" or health.get("status") == "warning":
                unhealthy.append({
                    "package": dep["name"],
                    "status": health.get("status"),
                    "issues": health.get("issues", []),
                    "last_checked": health.get("lastChecked")
                })
                
        if unhealthy:
            model["unhealthy_packages"] = unhealthy
            unhealthy_packages.append(model)
            
    return json.dumps({"models_with_unhealthy_packages": unhealthy_packages}, indent=2)

if __name__ == "__main__":
    if not CONFIG["is_connected"]:
        print("\n⚠️ Not connected to the discovery API.")
        print("Make sure environment variables are set and use 'connect()' to establish a connection.\n")
    
    mcp.run() 