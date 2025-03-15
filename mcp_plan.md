# dbt MCP Server key use cases

## Discovery API use cases

1. **Model Lineage and Impact Analysis**
   - Quickly understand upstream and downstream dependencies of any model
   - Identify which models would be affected by changes to a specific model
   - Get a complete lineage graph for any model in the project
   - Find all models that depend on a specific source table
   - Identify models that are part of critical business processes
   - Find models that are used in multiple downstream applications

2. **Documentation and Metadata Discovery**
   - Retrieve comprehensive documentation for any model including descriptions, columns, and tests
   - Find models with missing or incomplete documentation
   - Get a list of all models with specific tags or owners
   - Discover models that haven't been updated recently
   - Find models with missing column descriptions
   - Get a list of models with specific business domains or categories

3. **Quality and Testing Insights**
   - Identify models with failing tests or quality issues
   - Find models that are missing critical tests
   - Get a list of models with specific test types
   - Discover models with high failure rates in their test history
   - Find models with no tests at all
   - Identify models with custom test implementations

4. **Project Structure and Organization**
   - List all models in a specific schema or package
   - Find models that don't follow naming conventions
   - Get an overview of model distribution across different schemas
   - Identify orphaned models (no dependencies)
   - Find models that should be in different schemas based on their purpose
   - Get a list of models that violate project conventions

5. **Performance and Resource Usage**
   - Find models with long execution times
   - Identify models with high resource consumption
   - Get a list of models that frequently timeout
   - Discover models that could benefit from materialization changes
   - Find models that are materialized but could be views
   - Identify models with inefficient SQL patterns

6. **Collaboration and Ownership**
   - Find models owned by specific team members
   - Identify models that need review or updates
   - Get a list of models with pending changes
   - Discover models that haven't been touched in a while
   - Find models with outdated ownership information
   - Identify models that need to be transferred to new owners

7. **Source Data Analysis**
   - List all models that depend on specific source tables
   - Find models affected by source data changes
   - Get a list of models using deprecated sources
   - Discover models with direct source dependencies
   - Find models that should be using source tables instead of direct table references
   - Identify models with stale source freshness checks

8. **Package and Dependency Management**
   - Find models using specific dbt packages
   - Identify models with external dependencies
   - Get a list of models using deprecated package versions
   - Discover models that could be refactored to use newer package features
   - Find models that could benefit from additional package functionality
   - Identify models with conflicting package dependencies

9. **Business Impact Analysis**
   - Find models that are critical for business reporting
   - Identify models used in key business processes
   - Get a list of models with specific business value ratings
   - Discover models that are no longer needed
   - Find models that are duplicated across different business units
   - Identify models that need business review or approval

10. **Security and Access Control**
    - Find models with sensitive data
    - Identify models that need additional security measures
    - Get a list of models with specific access patterns
    - Discover models that violate security policies
    - Find models that need PII data handling
    - Identify models with inappropriate access levels

## Semantic Layer API use cases

1. **Metric Discovery and Exploration**
   - List all available metrics in a dbt project
   - Retrieve detailed metric definitions including descriptions and types
   - Find metrics by their type (simple, ratio, cumulative, derived)
   - Discover metrics with specific business context or domain
   - Get a list of metrics with their associated measures
   - Identify metrics that are part of critical business processes
   - Find metrics with specific measure configurations
   - Discover metrics with custom metric type parameters

2. **Dimension Management and Analysis**
   - List all available dimensions for specific metrics
   - Find dimensions by type (categorical, time)
   - Discover queryable granularities for time dimensions
   - Get dimensions that are part of specific semantic models
   - Identify dimensions used across multiple metrics
   - Find dimensions with specific business context
   - Discover partition dimensions
   - Get dimensions with custom expressions
   - Find dimensions with specific type parameters

3. **Time-Based Analysis**
   - Query metrics with different time grains (day, week, month, quarter, year)
   - Analyze metrics across different time periods
   - Compare metrics across different time dimensions
   - Find metrics with specific time-based aggregations
   - Identify metrics with custom time windows
   - Get metrics with cumulative time-based calculations
   - Find metrics with specific time dimension configurations
   - Discover metrics with custom time-based filters
   - Get metrics with specific time-based measure aggregations

4. **Complex Metric Calculations**
   - Query derived metrics with complex calculations
   - Analyze ratio metrics with custom numerators and denominators
   - Find cumulative metrics with specific window functions
   - Discover metrics with custom expressions
   - Get metrics with specific aggregation types
   - Identify metrics with custom filter conditions
   - Find metrics with specific metric type parameters
   - Discover metrics with custom window configurations
   - Get metrics with specific grain-to-date settings

5. **Multi-Dimensional Analysis**
   - Query metrics across multiple dimensions simultaneously
   - Analyze metrics with categorical and time dimensions together
   - Find metrics that can be analyzed by specific dimension combinations
   - Discover metrics with specific dimension hierarchies
   - Get metrics with custom dimension groupings
   - Identify metrics with specific dimension filters
   - Find metrics with multi-hop dimension relationships
   - Discover metrics with specific entity paths
   - Get metrics with custom dimension expressions

6. **Saved Query Management**
   - List all saved queries in the project
   - Find frequently used query patterns
   - Discover queries by business context
   - Get queries with specific metric combinations
   - Identify queries with custom filters and conditions
   - Find queries with specific time-based analysis
   - Discover queries with specific ordering
   - Get queries with custom limits
   - Find queries with specific compilation settings

7. **Data Platform Integration**
   - Identify the data platform dialect for the project
   - Find metrics compatible with specific data platforms
   - Discover platform-specific query optimizations
   - Get metrics with platform-specific aggregations
   - Identify metrics with custom platform functions
   - Find metrics with specific platform constraints
   - Discover platform-specific query patterns
   - Get metrics with specific platform configurations
   - Find metrics with custom platform expressions

8. **Query Optimization and Performance**
   - Analyze query patterns for optimization opportunities
   - Find metrics with high query complexity
   - Discover metrics that could benefit from caching
   - Get metrics with specific performance characteristics
   - Identify metrics with custom query hints
   - Find metrics with specific resource requirements
   - Discover metrics with specific query limits
   - Get metrics with custom query ordering
   - Find metrics with specific query compilation settings

9. **Business Intelligence Integration**
   - Find metrics suitable for BI tool integration
   - Discover metrics with specific visualization requirements
   - Get metrics with custom formatting rules
   - Identify metrics with specific display preferences
   - Find metrics with custom aggregation rules
   - Discover metrics with specific business context
   - Get metrics with specific query patterns
   - Find metrics with custom query parameters
   - Discover metrics with specific output formats

10. **Security and Access Control**
    - Find metrics with specific access patterns
    - Identify metrics with sensitive data
    - Discover metrics with custom security rules
    - Get metrics with specific user permissions
    - Find metrics with custom row-level security
    - Identify metrics with specific data masking requirements
    - Discover metrics with specific authentication requirements
    - Get metrics with custom access controls
    - Find metrics with specific security policies

11. **Query Execution and Results**
    - Execute queries with specific parameters
    - Get query results in various formats (Arrow, JSON)
    - Find queries with specific execution patterns
    - Discover queries with custom result formats
    - Get queries with specific pagination settings
    - Identify queries with custom result processing
    - Find queries with specific error handling
    - Discover queries with custom result transformations
    - Get queries with specific result validation

## Implementation Status

### Base Server Structure
- [x] Server initialization and configuration
- [x] Environment variable handling
- [x] Connection management
- [x] GraphQL query/mutation helpers

### Metric Discovery and Exploration
- [x] List all available metrics
- [x] Get detailed metric information
- [x] Find metrics by type
- [x] Get metric measures
- [x] Find metrics by domain
- [x] Get metric configurations
- [x] Find critical metrics

### Dimension Management and Analysis
- [x] List all available dimensions for specific metrics
- [x] Find dimensions by type (categorical, time)
- [x] Discover queryable granularities for time dimensions
- [x] Get dimensions that are part of specific semantic models
- [x] Identify dimensions used across multiple metrics
- [x] Find dimensions with specific business context
- [x] Discover partition dimensions
- [x] Get dimensions with custom expressions
- [x] Find dimensions with specific type parameters

### Time-Based Analysis
- [x] Query metrics with different time grains (day, week, month, quarter, year)
- [x] Analyze metrics across different time periods
- [x] Compare metrics across different time dimensions
- [x] Find metrics with specific time-based aggregations
- [x] Identify metrics with custom time windows
- [x] Get metrics with cumulative time-based calculations
- [x] Find metrics with specific time dimension configurations
- [x] Discover metrics with custom time-based filters
- [x] Get metrics with specific time-based measure aggregations

### Complex Metric Calculations
- [x] Query derived metrics with complex calculations
- [x] Analyze ratio metrics with custom numerators and denominators
- [x] Find cumulative metrics with specific window functions
- [x] Discover metrics with custom expressions
- [x] Get metrics with specific aggregation types
- [x] Identify metrics with custom filter conditions
- [x] Find metrics with specific metric type parameters
- [x] Discover metrics with custom window configurations
- [x] Get metrics with specific grain-to-date settings

### Multi-Dimensional Analysis
- [x] Query metrics across multiple dimensions simultaneously
- [x] Analyze metrics with categorical and time dimensions together
- [x] Find metrics that can be analyzed by specific dimension combinations
- [x] Discover metrics with specific dimension hierarchies
- [x] Get metrics with custom dimension groupings
- [x] Identify metrics with specific dimension filters
- [x] Find metrics with multi-hop dimension relationships
- [x] Discover metrics with specific entity paths
- [x] Get metrics with custom dimension expressions

### Saved Query Management
- [x] List all saved queries in the project
- [x] Find frequently used query patterns
- [x] Discover queries by business context
- [x] Get queries with specific metric combinations
- [x] Identify queries with custom filters and conditions
- [x] Find queries with specific time-based analysis
- [x] Discover queries with specific ordering
- [x] Get queries with custom limits
- [x] Find queries with specific compilation settings

### Data Platform Integration
- [x] Identify the data platform dialect for the project
- [x] Find metrics compatible with specific data platforms
- [x] Discover platform-specific query optimizations
- [x] Get metrics with platform-specific aggregations
- [x] Identify metrics with custom platform functions
- [x] Find metrics with specific platform constraints
- [x] Discover platform-specific query patterns
- [x] Get metrics with specific platform configurations
- [x] Find metrics with custom platform expressions

### Query Optimization and Performance
- [x] Analyze query patterns for optimization opportunities
- [x] Find metrics with high query complexity
- [x] Discover metrics that could benefit from caching
- [x] Get metrics with specific performance characteristics
- [x] Identify metrics with custom query hints
- [x] Find metrics with specific resource requirements
- [x] Discover metrics with specific query limits
- [x] Get metrics with custom query ordering
- [x] Find metrics with specific query compilation settings

### Business Intelligence Integration
- [x] Find metrics suitable for BI tool integration
- [x] Discover metrics with specific visualization requirements
- [x] Get metrics with custom formatting rules
- [x] Identify metrics with specific display preferences
- [x] Find metrics with custom aggregation rules
- [x] Discover metrics with specific business context
- [x] Get metrics with specific query patterns
- [x] Find metrics with custom query parameters
- [x] Discover metrics with specific output formats

### Security and Access Control
- [x] Find metrics with specific access patterns
- [x] Identify metrics with sensitive data
- [x] Discover metrics with custom security rules
- [x] Get metrics with specific user permissions
- [x] Find metrics with custom row-level security
- [x] Identify metrics with specific data masking requirements
- [x] Discover metrics with specific authentication requirements
- [x] Get metrics with custom access controls
- [x] Find metrics with specific security policies

### Query Execution and Results
- [x] Execute queries with specific parameters
- [x] Get query results in various formats (Arrow, JSON)
- [x] Find queries with specific execution patterns
- [x] Discover queries with custom result formats
- [x] Get queries with specific pagination settings
- [x] Identify queries with custom result processing
- [x] Find queries with specific error handling
- [x] Discover queries with custom result transformations
- [x] Get queries with specific result validation

### Discovery API Implementation Status

### Model Lineage and Impact Analysis
- [x] Get upstream and downstream dependencies
- [x] Identify affected models by changes
- [x] Get complete lineage graph
- [x] Find models depending on source tables
- [x] Identify critical business process models
- [x] Find multi-application models

### Documentation and Metadata Discovery
- [x] Get comprehensive model documentation
- [x] Find models with missing documentation
- [x] Get models by tags or owners
- [x] Find outdated models
- [x] Find models with missing column descriptions
- [x] Get models by business domain

### Quality and Testing Insights
- [x] Find models with failing tests
- [x] Find models missing critical tests
- [x] Get models by test type
- [x] Find models with high failure rates
- [x] Find untested models
- [x] Find models with custom tests

### Project Structure and Organization
- [x] List models by schema/package
- [x] Find models violating naming conventions
- [x] Get model distribution across schemas
- [x] Find orphaned models
- [x] Find misplaced models
- [x] Find convention-violating models

### Performance and Resource Usage
- [x] Find models with long execution times
- [x] Find models with high resource consumption
- [x] Find models that frequently timeout
- [x] Find models that could benefit from materialization changes
- [x] Find models that are materialized but could be views
- [x] Find models with inefficient SQL patterns

### Collaboration and Ownership
- [x] Find models by owner
- [x] Find models needing review
- [x] Find models with pending changes
- [x] Find stale models
- [x] Find models with outdated ownership
- [x] Find models needing ownership transfer

### Source Data Analysis
- [x] Find models by source dependencies
- [x] Find source-affected models
- [x] Find models using deprecated sources
- [x] Find direct source dependencies
- [x] Find models needing source tables
- [x] Find models with stale freshness checks

### Package and Dependency Management
- [x] Find models by package usage
- [x] Find models with external dependencies
- [x] Find models using deprecated packages
- [x] Find models needing package updates
- [x] Find models needing additional packages
- [x] Find models with package conflicts

### Business Impact Analysis
- [x] Find critical business models
- [x] Find key process models
- [x] Find models by business value
- [x] Find unused models
- [x] Find duplicated models
- [x] Find models needing business review

### Security and Access Control
- [x] Find models with sensitive data
- [x] Find models needing security measures
- [x] Find models by access pattern
- [x] Find security policy violations
- [x] Find models needing PII handling
- [x] Find models with access issues

### Summary of purpose

A Data-Centric View on Model Context Protocol Approach for AI Integration

Model Context Protocol (or MCP) is an open protocol that standardizes how applications provide context to LLMs. It is becoming a standard design for AI-driven applications, but let's talk about it from a data-first perspective. AI models need real-time, high-quality data, but we can't afford to let them hammer production databases or overload transactional systems with queries.

The solution? An indirect data access layer using MCP servers that connect to data lakes, vector databases, and pre-processed operational data instead of production resources.

🔗 How MCP Works in a Data-Centric Stack

MCP's architecture follows three main layers:

1️⃣ The Client Side: AI Model Requests
AI models (Claude, GPT, Replit AI, etc.) or applications (BI tools, LLM-powered dashboards) send queries via MCP. These clients don't access raw production data directly; they talk to MCP servers that handle data retrieval efficiently.

2️⃣ The Server Side: Data Layer Indirection

MCP servers act as an intermediary between AI models and critical data:

 ✅ Vector Databases (PostgreSQL, MongoDB, Oracle) → Fast retrieval for embeddings and semantic search.

 ✅ Data Lakes (S3, Delta Lake, Iceberg) → Batch-processed, structured data for AI to analyze.

Instead of directly querying an operational PostgreSQL or MongoDB instance, AI models query dedicated MCP servers that fetch cached, pre-processed, and AI-ready data.

3️⃣ The Protocol: Standardized AI-Data Access
MCP ensures:

 ✅ No direct polling of transactional databases (avoids performance hits)

 ✅ Pre-computed, AI-friendly datasets (low-latency responses)

 ✅ Query standardization across AI models (interoperability)

🚀 Why Data-Centric MCP Matters

1️⃣ Protects Critical Production Systems – No AI agent should be hammering your OLTP database for insights. MCP servers ensure queries hit AI-optimized data sources.

 2️⃣ Speeds Up AI Workflows – AI models don't wait for slow queries. Data lakes and vector DBs offer optimized search, improving response times.

 3️⃣ Keeps AI Outputs Reliable – By ensuring AI models interact with clean, versioned datasets, we avoid unpredictable outputs from stale or inconsistent data.

Example: AI-Powered Customer Insights
💡 A GPT-powered customer support agent needs to fetch past customer interactions. Instead of querying a live CRM, it queries an MCP server connected to:

 ✅ A vector database storing embeddings of past conversations for quick similarity search.

 ✅ A data lake holding batched customer interactions, aggregated daily for AI analysis.

Now, the AI agent retrieves insights in milliseconds without impacting production systems.