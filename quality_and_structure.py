#!/usr/bin/env python
"""
MCP Server for dbt Discovery API - Quality and Structure Functions

This module contains functions for analyzing model quality and project structure.
"""

import json
from mcp.server.fastmcp import FastMCP
from discovery_api_server import CONFIG, execute_query

# Create the MCP server
mcp = FastMCP("dbt Discovery API - Quality and Structure")

@mcp.tool()
def find_models_by_data_quality():
    """
    Find models with data quality issues or missing tests
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        columns {
          name
          description
          tests {
            name
            description
          }
        }
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with quality issues
    quality_issues = []
    for model in result.get("models", []):
        issues = []
        
        # Check for missing tests
        for column in model.get("columns", []):
            if not column.get("tests"):
                issues.append(f"Missing tests for column: {column['name']}")
                
        # Check for missing descriptions
        if not model.get("description"):
            issues.append("Missing model description")
        for column in model.get("columns", []):
            if not column.get("description"):
                issues.append(f"Missing description for column: {column['name']}")
                
        # Check for quality-related tags
        quality_tags = ["quality_issue", "needs_review", "data_problem"]
        if any(tag in model.get("tags", []) for tag in quality_tags):
            issues.append("Has quality-related tags")
            
        if issues:
            model["quality_issues"] = issues
            quality_issues.append(model)
            
    return json.dumps({"models_with_quality_issues": quality_issues}, indent=2)

@mcp.tool()
def find_models_by_test_coverage():
    """
    Get a list of models with their test coverage
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        columns {
          name
          description
          tests {
            name
            description
          }
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Calculate test coverage for each model
    coverage_data = []
    for model in result.get("models", []):
        total_columns = len(model.get("columns", []))
        tested_columns = sum(1 for col in model.get("columns", []) if col.get("tests"))
        
        coverage = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "total_columns": total_columns,
            "tested_columns": tested_columns,
            "coverage_percentage": round((tested_columns / total_columns * 100) if total_columns > 0 else 0, 2)
        }
        coverage_data.append(coverage)
        
    return json.dumps({"test_coverage": coverage_data}, indent=2)

@mcp.tool()
def find_models_by_schema_consistency():
    """
    Find models with schema inconsistencies or missing columns
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        schema
        columns {
          name
          description
          type
        }
        upstreamModels {
          name
          columns {
            name
            type
          }
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Check for schema inconsistencies
    inconsistencies = []
    for model in result.get("models", []):
        issues = []
        
        # Check upstream column consistency
        for upstream in model.get("upstreamModels", []):
            upstream_cols = {col["name"]: col["type"] for col in upstream.get("columns", [])}
            model_cols = {col["name"]: col["type"] for col in model.get("columns", [])}
            
            # Check for missing columns
            missing_cols = set(upstream_cols.keys()) - set(model_cols.keys())
            if missing_cols:
                issues.append(f"Missing columns from upstream model {upstream['name']}: {missing_cols}")
                
            # Check for type mismatches
            for col_name, col_type in model_cols.items():
                if col_name in upstream_cols and col_type != upstream_cols[col_name]:
                    issues.append(f"Type mismatch for column {col_name} with upstream {upstream['name']}")
                    
        if issues:
            model["schema_issues"] = issues
            inconsistencies.append(model)
            
    return json.dumps({"models_with_schema_issues": inconsistencies}, indent=2)

@mcp.tool()
def find_models_by_project_structure():
    """
    Get a list of models organized by project structure
    """
    if not CONFIG["is_connected"]:
        return "Not connected. Use connect() first."
    
    query = """{
      models {
        name
        description
        type
        schema
        path
        packageName
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Organize models by project structure
    structure = {
        "packages": {},
        "schemas": {},
        "paths": {}
    }
    
    for model in result.get("models", []):
        # Group by package
        package = model.get("packageName", "default")
        if package not in structure["packages"]:
            structure["packages"][package] = []
        structure["packages"][package].append(model)
        
        # Group by schema
        schema = model.get("schema", "default")
        if schema not in structure["schemas"]:
            structure["schemas"][schema] = []
        structure["schemas"][schema].append(model)
        
        # Group by path
        path = model.get("path", "default")
        if path not in structure["paths"]:
            structure["paths"][path] = []
        structure["paths"][path].append(model)
        
    return json.dumps({"project_structure": structure}, indent=2)

@mcp.tool()
def find_models_by_organization():
    """
    Get a list of models organized by business unit or department
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
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Organize models by business unit/department
    organization = {}
    
    for model in result.get("models", []):
        # Extract organization from tags or metadata
        org = None
        for tag in model.get("tags", []):
            if tag.startswith(("org.", "department.", "business_unit.")):
                org = tag.split(".")[1]
                break
                
        if not org and model.get("meta", {}).get("organization"):
            org = model["meta"]["organization"]
            
        if not org:
            org = "unassigned"
            
        if org not in organization:
            organization[org] = []
        organization[org].append(model)
        
    return json.dumps({"models_by_organization": organization}, indent=2)

@mcp.tool()
def find_models_by_complexity():
    """
    Find models with high complexity or potential maintenance issues
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
        }
        downstreamModels {
          name
        }
        columns {
          name
        }
        meta
        tags
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Analyze model complexity
    complex_models = []
    for model in result.get("models", []):
        complexity_score = 0
        issues = []
        
        # Count dependencies
        upstream_count = len(model.get("upstreamModels", []))
        downstream_count = len(model.get("downstreamModels", []))
        
        if upstream_count > 5:
            complexity_score += 2
            issues.append(f"High number of upstream dependencies ({upstream_count})")
            
        if downstream_count > 5:
            complexity_score += 2
            issues.append(f"High number of downstream dependencies ({downstream_count})")
            
        # Count columns
        column_count = len(model.get("columns", []))
        if column_count > 20:
            complexity_score += 1
            issues.append(f"Large number of columns ({column_count})")
            
        # Check for complexity-related tags
        complexity_tags = ["complex", "needs_refactoring", "high_maintenance"]
        if any(tag in model.get("tags", []) for tag in complexity_tags):
            complexity_score += 1
            issues.append("Marked as complex in tags")
            
        if complexity_score >= 2:
            model["complexity_score"] = complexity_score
            model["complexity_issues"] = issues
            complex_models.append(model)
            
    return json.dumps({"complex_models": complex_models}, indent=2)

if __name__ == "__main__":
    if not CONFIG["is_connected"]:
        print("\n⚠️ Not connected to the discovery API.")
        print("Make sure environment variables are set and use 'connect()' to establish a connection.\n")
    
    mcp.run() 