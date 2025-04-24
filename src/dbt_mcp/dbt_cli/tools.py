import subprocess

from mcp.server.fastmcp import FastMCP

from dbt_mcp.config.config import Config
from dbt_mcp.prompts.prompts import get_prompt


def register_dbt_cli_tools(dbt_mcp: FastMCP, config: Config) -> None:
    def _run_dbt_command(command: list[str]) -> str:
        # Add global CLI arguments from environment variable
        full_command = command.copy()
        if config.cli_args:
            # Insert CLI args after the main command but before command-specific args
            # This allows arguments like --quiet to work with all commands
            if len(full_command) > 0:
                main_command = full_command[0]
                command_args = full_command[1:] if len(full_command) > 1 else []
                full_command = [main_command, *config.cli_args, *command_args]
            else:
                full_command = [*config.cli_args]
        
        process = subprocess.Popen(
            args=[config.dbt_command, *full_command],
            cwd=config.project_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output, _ = process.communicate()
        return output

    @dbt_mcp.tool(description=get_prompt("dbt_cli/build"))
    def build() -> str:
        return _run_dbt_command(["build"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/compile"))
    def compile() -> str:
        return _run_dbt_command(["compile"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/docs"))
    def docs() -> str:
        return _run_dbt_command(["docs", "generate"])

    @dbt_mcp.tool(name="list", description=get_prompt("dbt_cli/list"))
    def ls() -> str:
        return _run_dbt_command(["list"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/parse"))
    def parse() -> str:
        return _run_dbt_command(["parse"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/run"))
    def run() -> str:
        return _run_dbt_command(["run"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/test"))
    def test() -> str:
        return _run_dbt_command(["test"])

    @dbt_mcp.tool(description=get_prompt("dbt_cli/show"))
    def show(sql_query: str, limit: int | None = None) -> str:
        # For 'show' command, we need special handling to ensure SQL query is properly positioned
        # First part of the command (before any CLI args would be inserted)
        args = ["show"]
        
        # Second part of the command (after CLI args would be inserted)
        query_args = ["--inline", sql_query, "--favor-state"]
        if limit:
            query_args.extend(["--limit", str(limit)])
        query_args.extend(["--output", "json"])
        
        # Insert CLI args after the 'show' command but before the query arguments
        full_command = args.copy()
        if config.cli_args:
            full_command.extend(config.cli_args)
        full_command.extend(query_args)
        
        return _run_dbt_command(full_command)
