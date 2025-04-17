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

# This script is designed to be run on macOS.

mcp_server_dir="${HOME}/.dbt-mcp"

function check_existing_installation() {
    if [[ -d "${mcp_server_dir}" && -f "${mcp_server_dir}/.venv/bin/dbt-mcp" ]]; then
        echo "dbt-mcp is already installed in ${mcp_server_dir}."
        echo "How would you like to proceed?"
        echo "1. Remove the existing installation and start fresh"
        echo "2. Try to update the existing installation"
        echo "3. Abort! Abort!"

        read -p "Enter your choice (1-3): " choice

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
        . .venv/bin/activate
        pip install --upgrade dbt-mcp
        echo "Update completed."
        echo "We hope you'll like the new version!"
    else
        echo "Error: Failed to activate virtual environment."
        echo "Please choose option 1 to start fresh."
        exit 1
    fi
}

function python() {
    # check if it's python or python3
    if command -v python3 &>/dev/null; then
        echo "python3"
    else
        echo "python"
    fi
}

function check_python() {
    # check if python is installed
    if ! command -v $(python) &>/dev/null; then
        echo "Python is not installed. Please install Python and try again."
        exit 1
    fi

    # check if python is version 3.12 
    python_version=$($(python) --version 2>&1 | awk '{print $2}')
    if [[ ! "${python_version}" =~ 3\.12 ]]; then
        echo "Python version ${python_version} is not supported. Please install Python 3.12."
        exit 1
    fi
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
    if [[ -z "${target_package}" ]]; then
        target_package="dbt-mcp"
    fi

    current_dir=$(pwd)
    mkdir -p "${mcp_server_dir}"
    cd "${mcp_server_dir}" || exit 1
    $(python) -m venv .venv
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
    # Create .env file
    echo "------------------------------------------------------------------"
    echo "We need a couple of details about your dbt project to get started."
    echo "------------------------------------------------------------------"
    touch "${mcp_server_dir}/.env"

    # Prompt for environment variables with defaults
    DBT_HOST=$(prompt_with_default "Enter DBT_HOST" "cloud.dbt.com")
    DBT_ENV_ID=$(prompt_with_default "Enter DBT_ENV_ID" "1")
    DBT_TOKEN=$(prompt_with_default "Enter DBT_TOKEN" "")
    DBT_EXECUTABLE_TYPE=$(prompt_with_default "Enter DBT_EXECUTABLE_TYPE" "cloud")

    # Write to .env file
    cat >"${mcp_server_dir}/.env" <<EOF
DBT_HOST=${DBT_HOST}
DBT_ENV_ID=${DBT_ENV_ID}
DBT_TOKEN=${DBT_TOKEN}
DBT_EXECUTABLE_TYPE=${DBT_EXECUTABLE_TYPE}
EOF

    echo "Great! That's all we needed for now."
    echo "You can always adjust the configuration later in ${mcp_server_dir}/.env"
}

# make sure python is installed and has version 3.12 or higher
check_python

# check if mcp-dbt is already installed
check_existing_installation

# install dbt-mcp package
install_dbt_mcp_package "$1"

# configure environment
configure_environment

echo "Installation and configuration complete!"
echo "Have a great day!"
