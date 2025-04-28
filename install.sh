#!/bin/bash

# This script installs and manages the dbt-mcp server installation.
# It handles fresh installations, updates, and removals of the dbt-mcp package
# in a virtual environment under the user's home directory.
#
# Copyright 2025 dbt Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

mcp_server_dir="${HOME}/.dbt-mcp"
config_file="${mcp_server_dir}/.env"

function check_existing_installation() {
    if [[ -d "${mcp_server_dir}" && -f "${mcp_server_dir}/.venv/bin/dbt-mcp" ]]; then
        echo "dbt-mcp is already installed at ${mcp_server_dir}."
        echo ""
        echo "How would you like to proceed?"
        echo "1. Remove the existing installation and start fresh"
        echo "2. Try to update the existing installation"
        echo "3. Reconfigure the existing installation"
        echo "4. Show the current configuration"
        echo "5. Abort! Abort!"

        read -p "Enter your choice (1-5): " choice

        case "${choice}" in
        1)
            echo "Removing existing installation..."
            rm -rf "${mcp_server_dir}"
            echo "Existing installation removed. Continuing with fresh install..."
            ;;
        2)
            update_existing_installation
            exit 0
            ;;
        3)
            echo "This will override the existing configuration. Are you sure you want to proceed?"
            read -p "Enter y/n: " confirm
            if [[ "${confirm}" =~ ^[Yy]$ ]]; then
                rm -rf "${config_file}"
                configure_environment
                render_mcp_config
                exit 0
            else
                echo "Configuration not changed. Exiting."
                exit 0
            fi
            ;;
        4)
            echo "Current configuration:"
            render_mcp_config
            exit 0
            ;;
        5)
            echo "Installation aborted. Bye!"
            exit 0
            ;;
        *)
            echo "Invalid choice. Please enter 1, 2, or 3."
            exit 1
            ;;
        esac
    fi
}

function update_existing_installation() {
    echo "Attempting to update existing installation..."
    cd "${mcp_server_dir}" || exit 1
    if [[ -f ".venv/bin/activate" ]]; then
        source .venv/bin/activate
        pip install --upgrade dbt-mcp
        echo "Update completed."
        echo "We hope you'll like the new version!"
    else
        echo "Error: Failed to activate virtual environment."
        echo "Please choose option 1 to start fresh."
        exit 1
    fi
}

function check_python() {
    # check if python is installed
    if ! command -v python &>/dev/null; then
        echo "Python is not installed. Please install Python and try again."
        exit 1
    fi

    # check if python version is 3.12 or higher
    python_version=$(python --version 2>&1 | awk '{print $2}')
    major_version=$(echo "${python_version}" | cut -d. -f1)
    minor_version=$(echo "${python_version}" | cut -d. -f2)

    if [[ "${major_version}" -lt 3 ]] || { [[ "${major_version}" -eq 3 ]] && [[ "${minor_version}" -lt 12 ]]; }; then
        echo "Python version ${python_version} is not supported. Please install Python 3.12 or higher"
        exit 1
    fi

    echo "Using Python version: ${python_version}"
}

# Function to prompt for input with a default value
prompt_with_default() {
    local prompt="$1"
    local default="$2"

    if [[ -z "${default}" ]]; then
        default_text=""
    else
        default_text=" [${default}]"
    fi

    read -p "${prompt}${default_text}: " input

    if [[ -z "${input}" ]]; then
        input="${default}"
    fi
    echo "${input}"
}

function install_dbt_mcp_package() {
    local target_package="$1"
    # sanity check to make sure the target package is dbt-mcp
    if [[ -n "${target_package}" && ! "${target_package}" =~ "dbt-mcp" ]]; then
        echo "========================================================="
        echo "Hold on! This does not look like a valid dbt-mcp package."
        echo "========================================================="
        read -p "Are you sure you want to install ${target_package}? (y/n): " confirm
        if [[ ! "${confirm}" =~ ^[Yy]$ ]]; then
            echo "Installation aborted. Bye!"
            exit 0
        fi
    fi

    if [[ -z "${target_package}" ]]; then
        target_package="dbt-mcp"
    fi

    current_dir=$(pwd)
    mkdir -p "${mcp_server_dir}"
    cd "${mcp_server_dir}" || exit 1
    python -m venv .venv
    if [[ -f ".venv/bin/activate" ]]; then
        source .venv/bin/activate
        if ! pip install "${target_package}"; then
            echo "Error: Failed to install ${target_package}"
            exit 1
        fi
        deactivate
        ln -s "${mcp_server_dir}/.venv/bin/dbt-mcp" dbt-mcp
        cd "${current_dir}" || exit 1
        echo "dbt-mcp installed in ${mcp_server_dir}"
    else
        echo "Error: Failed to activate virtual environment."
        exit 1
    fi
}

function configure_environment() {
    echo ""
    echo "-----------------------------------------------------------------------"
    echo "We need a couple of details about your dbt project to get started."
    echo "You can always adjust the configuration later in ${config_file}"
    echo "-----------------------------------------------------------------------"
    echo ""
    touch "${config_file}"

    config_options=()
    echo "Do you have a local dbt project and want the MCP server use it?"
    read -p "Enter y/n: " local_dbt_project
    if [[ "${local_dbt_project}" =~ ^[Yy]$ ]]; then
        config_options+=("DBT_PROJECT_DIR;$(pwd)")
        config_options+=("DBT_PATH;$(which dbt)")
    else
        echo "DISABLE_DBT_CLI=true" >>"${config_file}"
    fi

    echo "Do you have a dbt Cloud account and want the MCP server to access it?"
    read -p "Enter y/n: " dbt_cloud
    if [[ "${dbt_cloud}" =~ ^[Yy]$ ]]; then
        config_options+=("DBT_HOST;https://cloud.getdbt.com")
        config_options+=("DBT_TOKEN")
        config_options+=("DBT_PROD_ENV_ID")

        echo "Do you want to give the MCP server access to the discovery API?"
        read -p "Enter y/n: " discovery
        if [[ "${discovery}" =~ ^[Nn]$ ]]; then
            echo "DISABLE_DISCOVERY=true" >>"${config_file}"
        fi

        echo "Do you want to configure the MCP server to use your semantic layer?"
        read -p "Enter y/n: " semantic_layer
        if [[ "${semantic_layer}" =~ ^[Nn]$ ]]; then
            echo "DISABLE_SEMANTIC_LAYER=true" >>"${config_file}"
        fi

        echo "Do you want to configure the MCP server to use remote tools?"
        read -p "Enter y/n: " remote_tools
        if [[ "${remote_tools}" =~ ^[Yy]$ ]]; then
            config_options+=("DBT_USER_ID")
            config_options+=("DBT_DEV_ENV_ID")
        else
            echo "DISABLE_REMOTE_TOOLS=true" >>"${config_file}"
        fi
    else
        cat >"${config_file}" <<EOF
DISABLE_SEMANTIC_LAYER=true
DISABLE_DISCOVERY=true
DISABLE_REMOTE_TOOLS=true
EOF
    fi
    echo ""
    echo "Please set the following environment variables:"
    for option in "${config_options[@]}"; do
        option_name=$(echo "${option}" | cut -d';' -f1)
        echo "${option_name}"
    done
    echo "Consult the dbt-mcp documentation for more information on what each of these variables do."

    echo ""
    read -p "Do you have the values for the above variables ready and want to configure them right now? (y/n): " configure_now
    if [[ "${configure_now}" =~ ^[Yy]$ ]]; then

        for option in "${config_options[@]}"; do
            # if option contains ; split otherwise set to empty string
            if [[ "${option}" =~ ";" ]]; then
                option_name=$(echo "${option}" | cut -d';' -f1)
                default_value=$(echo "${option}" | cut -d';' -f2)
            else
                option_name="${option}"
                default_value=""
            fi
            config_value=$(prompt_with_default "Enter ${option_name}" "${default_value}")

            if [[ "${option_name}" == "DBT_HOST" ]]; then
                # trim https:// and trailing slashes
                config_value=$(echo "${config_value}" | sed 's/^https:\/\///' | sed 's/\/$//')

                # split hostname by .
                hostname_parts=(${config_value//./ })

                # if the number of elements is 4 or more, we have a cell based hostname
                if [[ ${#hostname_parts[@]} -ge 4 && "${config_value}" =~ dbt\.com$ ]]; then
                    cell=${hostname_parts[0]}
                    echo "MULTICELL_ACCOUNT_PREFIX=${cell}" >>"${config_file}"

                    # trim cell from config_value and echo to config_file
                    config_value=$(echo "${config_value}" | sed "s/${cell}\.//")
                fi
            fi
            echo "${option_name}=${config_value}" >>"${config_file}"
        done
    else
        for option in "${config_options[@]}"; do
            option_name=$(echo "${option}" | cut -d';' -f1)
            option_value=$(echo "${option}" | cut -d';' -f2)
            if [[ "${option_name}" == "${option_value}" ]]; then
                option_value="<${option_name}>"
            fi
            echo "${option_name}=${option_value}" >>"${config_file}"
        done
    fi
    echo ""
    echo "Great! That's all we needed for now."
    echo "You can always adjust the configuration later in ${config_file}"
}

function render_mcp_config() {
    config=$(cat "${config_file}")
    env_vars=()
    for line in ${config}; do
        key=$(echo "${line}" | cut -d= -f1)
        value=$(echo "${line}" | cut -d= -f2)
        env_vars+=("\"${key}\": \"${value}\"")
    done
    env_vars_str=$(printf '        %s,\n' "${env_vars[@]}" | sed '$s/,$//')
    echo ""
    echo "Use the following to configure the MCP server in the tool of your choice:"
    echo "{
  \"mcpServers\": {
    \"dbt-mcp\": {
      \"command\": \"${mcp_server_dir}/dbt-mcp\",
      \"env\": {
${env_vars_str}
      }
    }
  }
}"
}

# make sure python is installed and has version 3.12 or higher
check_python

# check if mcp-dbt is already installed
check_existing_installation

# install dbt-mcp package
install_dbt_mcp_package "$1"

configure_environment

render_mcp_config

echo "Installation and configuration complete!"
echo "Have a great day!"
