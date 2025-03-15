#!/usr/bin/env python
"""
MCP Server for dbt Discovery API - Performance and Collaboration Functions

This module contains functions for analyzing model performance and collaboration patterns.
"""

import json
import time
from mcp.server.fastmcp import FastMCP
from discovery_api_server import CONFIG, execute_query

# Create the MCP server
mcp = FastMCP("dbt Discovery API - Performance and Collaboration")

@mcp.tool()
def find_models_by_performance():
    """
    Find models with performance issues or long execution times
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
        lastRun {
          status
          executionTime
          startedAt
          completedAt
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with performance issues
    performance_issues = []
    for model in result.get("models", []):
        issues = []
        last_run = model.get("lastRun", {})
        
        # Check execution time
        if last_run.get("executionTime", 0) > 300:  # More than 5 minutes
            issues.append(f"Long execution time: {last_run['executionTime']} seconds")
            
        # Check for failed runs
        if last_run.get("status") == "error":
            issues.append("Last run failed")
            
        # Check for performance-related tags
        perf_tags = ["slow", "needs_optimization", "performance_issue"]
        if any(tag in model.get("tags", []) for tag in perf_tags):
            issues.append("Marked as having performance issues")
            
        if issues:
            model["performance_issues"] = issues
            performance_issues.append(model)
            
    return json.dumps({"models_with_performance_issues": performance_issues}, indent=2)

@mcp.tool()
def find_models_by_resource_usage():
    """
    Find models with high resource usage or memory consumption
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
        lastRun {
          status
          executionTime
          memoryUsage
          startedAt
          completedAt
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with high resource usage
    resource_issues = []
    for model in result.get("models", []):
        issues = []
        last_run = model.get("lastRun", {})
        
        # Check memory usage
        if last_run.get("memoryUsage", 0) > 1024 * 1024 * 1024:  # More than 1GB
            issues.append(f"High memory usage: {last_run['memoryUsage'] / (1024*1024*1024):.2f} GB")
            
        # Check for resource-related tags
        resource_tags = ["high_memory", "resource_intensive", "needs_optimization"]
        if any(tag in model.get("tags", []) for tag in resource_tags):
            issues.append("Marked as resource intensive")
            
        if issues:
            model["resource_issues"] = issues
            resource_issues.append(model)
            
    return json.dumps({"models_with_resource_issues": resource_issues}, indent=2)

@mcp.tool()
def find_models_by_collaboration():
    """
    Find models with multiple contributors or ownership patterns
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
        owner
        contributors {
          name
          email
          role
        }
        lastModified
        lastModifiedBy
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Analyze collaboration patterns
    collaboration_data = []
    for model in result.get("models", []):
        collab_info = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "owner": model.get("owner"),
            "contributors": model.get("contributors", []),
            "last_modified": model.get("lastModified"),
            "last_modified_by": model.get("lastModifiedBy"),
            "collaboration_score": 0
        }
        
        # Calculate collaboration score
        if len(model.get("contributors", [])) > 1:
            collab_info["collaboration_score"] += 2
            
        if model.get("lastModifiedBy") and model.get("lastModifiedBy") != model.get("owner"):
            collab_info["collaboration_score"] += 1
            
        if model.get("meta", {}).get("team"):
            collab_info["collaboration_score"] += 1
            
        collaboration_data.append(collab_info)
        
    return json.dumps({"collaboration_analysis": collaboration_data}, indent=2)

@mcp.tool()
def find_models_by_ownership():
    """
    Find models with ownership issues or unclear ownership
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
        owner
        contributors {
          name
          email
          role
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with ownership issues
    ownership_issues = []
    for model in result.get("models", []):
        issues = []
        
        # Check for missing owner
        if not model.get("owner"):
            issues.append("No owner assigned")
            
        # Check for ownership in metadata
        if not model.get("meta", {}).get("owner"):
            issues.append("No owner in metadata")
            
        # Check for ownership-related tags
        ownership_tags = ["needs_owner", "unassigned", "orphaned"]
        if any(tag in model.get("tags", []) for tag in ownership_tags):
            issues.append("Marked as needing ownership")
            
        if issues:
            model["ownership_issues"] = issues
            ownership_issues.append(model)
            
    return json.dumps({"models_with_ownership_issues": ownership_issues}, indent=2)

@mcp.tool()
def find_models_by_activity():
    """
    Find models with high or low activity levels
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
        lastModified
        lastModifiedBy
        lastRun {
          status
          startedAt
          completedAt
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Analyze activity patterns
    activity_data = []
    current_time = time.time()
    
    for model in result.get("models", []):
        activity_info = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "last_modified": model.get("lastModified"),
            "last_modified_by": model.get("lastModifiedBy"),
            "last_run": model.get("lastRun", {}),
            "activity_status": "unknown"
        }
        
        # Determine activity status
        last_modified = model.get("lastModified")
        if last_modified:
            days_since_modified = (current_time - last_modified) / (24 * 60 * 60)
            if days_since_modified < 7:
                activity_info["activity_status"] = "high"
            elif days_since_modified < 30:
                activity_info["activity_status"] = "medium"
            else:
                activity_info["activity_status"] = "low"
                
        activity_data.append(activity_info)
        
    return json.dumps({"activity_analysis": activity_data}, indent=2)

@mcp.tool()
def find_models_by_team_collaboration():
    """
    Find models with cross-team collaboration patterns
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
        owner
        contributors {
          name
          email
          role
          team
        }
        lastModifiedBy
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Analyze team collaboration
    team_collab_data = []
    for model in result.get("models", []):
        collab_info = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "owner": model.get("owner"),
            "contributors": model.get("contributors", []),
            "last_modified_by": model.get("lastModifiedBy"),
            "teams_involved": set()
        }
        
        # Collect teams involved
        if model.get("owner"):
            collab_info["teams_involved"].add(model["owner"].split("@")[1].split(".")[0])
            
        for contributor in model.get("contributors", []):
            if contributor.get("team"):
                collab_info["teams_involved"].add(contributor["team"])
                
        if model.get("lastModifiedBy"):
            collab_info["teams_involved"].add(model["lastModifiedBy"].split("@")[1].split(".")[0])
            
        collab_info["teams_involved"] = list(collab_info["teams_involved"])
        collab_info["cross_team"] = len(collab_info["teams_involved"]) > 1
        
        team_collab_data.append(collab_info)
        
    return json.dumps({"team_collaboration_analysis": team_collab_data}, indent=2)

if __name__ == "__main__":
    if not CONFIG["is_connected"]:
        print("\n⚠️ Not connected to the discovery API.")
        print("Make sure environment variables are set and use 'connect()' to establish a connection.\n")
    
    mcp.run() 