# dbt MCP Server

This MCP (Model Context Protocol) server provides MCP tools to interact with dbt in a few different ways.

In its current form, it allows users to:
- run commands from their local install of the dbt Core or dbt Cloud CLI
- get information about their models and the transformation configured in a given dbt project
- interact with the dbt Cloud Semantic Layer gateway, getting the list of metrics, dimensions and directly querying those

## Architecture

![architecture diagram of the dbt MCP server](https://github.com/user-attachments/assets/89b8a24b-da7b-4e54-ba48-afceaa56f956)

## Setup

1. Clone the repository:
```shell
git clone https://github.com/dbt-labs/dbt-mcp.git
cd dbt-mcp
```

2. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

3. [Install Task](https://taskfile.dev/installation/)

4. Run `task install`

5. Configure environment variables:
```shell
cp .env.example .env
```
Then edit `.env` with your specific environment variables (see Configuration)

### Portable development environment

You can also use the supplied development environment, which includes all the required tools.

#### Visual Studio Code devcontainer

If you use Visual Studio Code and containers, you can open the command palette (`Shift+Cmd+P` on mac) and select `Dev Containers: Rebuild and Reopen in Container`. This will spin up the devcontainer.

#### devbox

If you use [devbox](https://github.com/jetify-com/devbox), you can start the devbox shell by running:

```sh
devbox shell
```

## Installation

Want to get going quickly?

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/dbt-labs/dbt-mcp/refs/heads/main/install.sh)"
```
The installer also serves as an updater, simply run it again and it will detect your exisiting dbt-mcp installation and offer to update it.

## Configuration

The MCP server takes the following configuration:

- `DISABLE_DBT_CLI`: Set this to `true` to disable dbt Core and dbt Cloud CLI MCP objects. Otherwise, they are enabled.
- `DISABLE_SEMANTIC_LAYER`: Set this to `true` to disable dbt Semantic Layer MCP objects. Otherwise, they are enabled.
- `DISABLE_DISCOVERY`: Set this to `true` to disable dbt Discovery API MCP objects. Otherwise, they are enabled.
- `DISABLE_REMOTE`: Set this to `false` to enable remote MCP objects. They are disable by default.
- `DBT_HOST`: Your dbt Cloud instance hostname. This will look like an `Access URL` found [here](https://docs.getdbt.com/docs/cloud/about-cloud/access-regions-ip-addresses). If you are using Multi-cell, do not include the `ACCOUNT_PREFIX` here.
- `MULTICELL_ACCOUNT_PREFIX`: If you are using Multi-cell, set this to your `ACCOUNT_PREFIX`. If you are not using Multi-cell, do not set this environment variable. You can learn more [here](https://docs.getdbt.com/docs/cloud/about-cloud/access-regions-ip-addresses).
- `DBT_PROD_ENV_ID`: Your dbt Cloud production environment ID.
- `DBT_DEV_ENV_ID`: Your dbt Cloud development environment ID.
- `DBT_USER_ID`: Your dbt Cloud user ID.
- `DBT_TOKEN`: Your personal access token or service token. Service token is required when using the Semantic Layer.
- `DBT_PROJECT_DIR`: The path to your dbt Project.
- `DBT_PATH`: The path to your dbt Core or dbt Cloud CLI executable. You can find your dbt executable by running `which dbt`.
- `DBT_EXECUTABLE_TYPE`: Set this to `core` if the `DBT_PATH` environment variable points toward dbt Core. Otherwise, dbt Cloud CLI is assumed

## Using with MCP Clients

After going through [Setup](#setup), you can use your server with an MCP client.

This configuration will be added to the respective client's config file:

```json
{
  "mcpServers": {
    "dbt": {
      "command": "<path-to-this-directory>/.venv/bin/mcp",
      "args": [
        "run",
        "<path-to-this-directory>/src/dbt_mcp/main.py"
      ]
    }
  }
}
```
Be sure to replace `<path-to-this-directory>`

If you encounter any problems. You can try running `task run` to see errors in your terminal


## Claude Desktop

Follow [these](https://modelcontextprotocol.io/quickstart/user) instructions to create the `claude_desktop_config.json` file and connect.

You can find the Claude Desktop logs at `~/Library/Logs/Claude`.


## Cursor

1. Open the Cursor menu and select Settings → Cursor Settings → MCP
2. Click "Add new global MCP server"
3. Add the config from above to the provided `mcp.json` file
4. Verify your connection is active within the MCP tab

Cursor MCP docs [here](https://docs.cursor.com/context/model-context-protocol) for reference


## VS Code

1. Open the Settings menu (Command + Comma) and select the correct tab atop the page for your use case
    - `Workspace` - configures the server in the context of your workspace
    - `User` - configures the server in the context of your user
2. Select Features → Chat
3. Ensure that "Mcp" is `Enabled`
![mcp-vscode-settings](https://github.com/user-attachments/assets/3d3fa853-2398-422a-8a6d-7f0a97120aba)


4. Click "Edit in settings.json" under "Mcp > Discovery"

5. Add your server configuration (`dbt`) to the provided `settings.json` file as one of the servers
```json
{
    "mcp": {
        "inputs": [],
        "servers": {
          "dbt": {
            "command": "<path-to-this-directory>/.venv/bin/mcp",
            "args": ["run", "<path-to-this-directory>/src/dbt_mcp/main.py"]
          }
        }
    }
}
```

After setup you can start, stop, and configure your MCP servers by:
- Running the `MCP: List Servers` command from the Command Palette (Control + Command + P) and selecting the server
- Utlizing the keywords inline within the `settings.json` file

![inline-management](https://github.com/user-attachments/assets/d33d4083-5243-4b36-adab-72f12738c263)

VS Code MCP docs [here](https://code.visualstudio.com/docs/copilot/chat/mcp-servers) for reference


## Tools

### dbt CLI

* `build` - Executes models, tests, snapshots, and seeds in dependency order
* `compile` - Generates executable SQL from models, tests, and analyses without running them
* `docs` - Generates documentation for the dbt project
* `ls` (list) - Lists resources in the dbt project, such as models and tests
* `parse` - Parses and validates the project’s files for syntax correctness
* `run` -  Executes models to materialize them in the database
* `test` - Runs tests to validate data and model integrity
* `show` - Runs a query against the data warehouse

> Allowing your client to utilize dbt commands through this MCP tooling could modify your data models, sources, and warehouse objects. Proceed only if you trust the client and understand the potential impact.


### Semantic Layer

* `list_metrics` - Retrieves all defined metrics
* `get_dimensions` - Gets dimensions associated with specified metrics
* `get_entities` - Gets entities associated with specified metrics
* `query_metrics` - Queries metrics with optional grouping, ordering, filtering, and limiting


### Discovery
* `get_mart_models` - Gets all mart models
* `get_all_models` - Gets all models
* `get_model_details` - Gets details for a specific model
* `get_model_parents` - Gets parent models of a specific model


## Contributing

Read `CONTRIBUTING.md` for instructions on how to get involved!
