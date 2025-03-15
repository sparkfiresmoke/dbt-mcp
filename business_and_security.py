#!/usr/bin/env python
"""
MCP Server for dbt Discovery API - Business Impact and Security Functions

This module contains functions for analyzing business impact and security patterns.
"""

import json
from mcp.server.fastmcp import FastMCP
from dbt_api_utils import CONFIG, execute_query, format_json_response

# Create the MCP server
mcp = FastMCP("dbt Discovery API - Business Impact and Security")

@mcp.tool()
def find_models_by_business_impact():
    """
    Find models with high business impact or critical business processes
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
        downstreamModels {
          name
          type
          meta
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with high business impact
    high_impact_models = []
    for model in result.get("models", []):
        impact_score = 0
        impact_factors = []
        
        # Check for business-critical tags
        critical_tags = ["business_critical", "key_process", "high_impact", "revenue"]
        if any(tag in model.get("tags", []) for tag in critical_tags):
            impact_score += 3
            impact_factors.append("Marked as business critical")
            
        # Check downstream impact
        downstream_count = len(model.get("downstreamModels", []))
        if downstream_count > 5:
            impact_score += 2
            impact_factors.append(f"High downstream impact ({downstream_count} models)")
            
        # Check for revenue impact
        if model.get("meta", {}).get("revenue_impact"):
            impact_score += 2
            impact_factors.append("Direct revenue impact")
            
        if impact_score >= 3:
            model["impact_score"] = impact_score
            model["impact_factors"] = impact_factors
            high_impact_models.append(model)
            
    return json.dumps({"high_impact_models": high_impact_models}, indent=2)

@mcp.tool()
def find_models_by_business_domain():
    """
    Find models organized by business domain or function
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
        
    # Organize models by business domain
    domain_models = {}
    for model in result.get("models", []):
        # Extract domain from tags or metadata
        domain = None
        for tag in model.get("tags", []):
            if tag.startswith(("domain.", "business_unit.", "function.")):
                domain = tag.split(".")[1]
                break
                
        if not domain and model.get("meta", {}).get("domain"):
            domain = model["meta"]["domain"]
            
        if not domain:
            domain = "unassigned"
            
        if domain not in domain_models:
            domain_models[domain] = []
        domain_models[domain].append(model)
        
    return json.dumps({"models_by_domain": domain_models}, indent=2)

@mcp.tool()
def find_models_by_business_rules():
    """
    Find models implementing critical business rules
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
        
    # Filter for models with business rules
    business_rules = []
    for model in result.get("models", []):
        rules = []
        
        # Check for business rule tags
        rule_tags = ["business_rule", "validation", "compliance"]
        if any(tag in model.get("tags", []) for tag in rule_tags):
            rules.append("Implements business rules")
            
        # Check for validation tests
        for column in model.get("columns", []):
            for test in column.get("tests", []):
                if test.get("name") in ["unique", "not_null", "accepted_values"]:
                    rules.append(f"Validation rule for column {column['name']}")
                    
        if rules:
            model["business_rules"] = rules
            business_rules.append(model)
            
    return json.dumps({"models_with_business_rules": business_rules}, indent=2)

@mcp.tool()
def find_models_by_security_level():
    """
    Find models with specific security requirements or sensitive data
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
        
    # Filter for models with security requirements
    security_models = []
    for model in result.get("models", []):
        security_info = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "security_level": "standard",
            "security_measures": []
        }
        
        # Check for security-related tags
        security_tags = ["pii", "confidential", "restricted", "high_security"]
        if any(tag in model.get("tags", []) for tag in security_tags):
            security_info["security_level"] = "high"
            security_info["security_measures"].append("Marked as sensitive data")
            
        # Check for security-related tests
        for column in model.get("columns", []):
            for test in column.get("tests", []):
                if test.get("name") in ["unique", "not_null"]:
                    security_info["security_measures"].append(f"Data validation for {column['name']}")
                    
        if security_info["security_measures"]:
            security_models.append(security_info)
            
    return json.dumps({"models_with_security_requirements": security_models}, indent=2)

@mcp.tool()
def find_models_by_access_control():
    """
    Find models with specific access control requirements
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
        access {
          level
          groups
          roles
        }
      }
    }"""
    
    result = execute_query(query)
    if isinstance(result, str):
        return result
        
    # Filter for models with access control
    access_controlled = []
    for model in result.get("models", []):
        access_info = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "access_level": "standard",
            "access_controls": []
        }
        
        # Check access control metadata
        access = model.get("access", {})
        if access.get("level") in ["restricted", "confidential"]:
            access_info["access_level"] = access["level"]
            access_info["access_controls"].append(f"Restricted access level: {access['level']}")
            
        # Check for access-related tags
        access_tags = ["restricted_access", "confidential", "need_to_know"]
        if any(tag in model.get("tags", []) for tag in access_tags):
            access_info["access_controls"].append("Marked as restricted access")
            
        if access_info["access_controls"]:
            access_controlled.append(access_info)
            
    return json.dumps({"models_with_access_controls": access_controlled}, indent=2)

@mcp.tool()
def find_models_by_compliance():
    """
    Find models with compliance requirements or regulatory needs
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
        
    # Filter for models with compliance requirements
    compliance_models = []
    for model in result.get("models", []):
        compliance_info = {
            "model_name": model["name"],
            "description": model.get("description"),
            "type": model.get("type"),
            "compliance_requirements": []
        }
        
        # Check for compliance-related tags
        compliance_tags = ["gdpr", "hipaa", "sox", "pci", "compliance"]
        if any(tag in model.get("tags", []) for tag in compliance_tags):
            compliance_info["compliance_requirements"].append("Marked as compliance-related")
            
        # Check for compliance-related tests
        for column in model.get("columns", []):
            for test in column.get("tests", []):
                if test.get("name") in ["unique", "not_null", "accepted_values"]:
                    compliance_info["compliance_requirements"].append(f"Data validation for {column['name']}")
                    
        if compliance_info["compliance_requirements"]:
            compliance_models.append(compliance_info)
            
    return json.dumps({"models_with_compliance_requirements": compliance_models}, indent=2)

if __name__ == "__main__":
    if not CONFIG["is_connected"]:
        print("\n⚠️ Not connected to the discovery API.")
        print("Make sure environment variables are set and use 'connect()' to establish a connection.\n")
    
    mcp.run() 