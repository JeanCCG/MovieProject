# Getting Started

Follow these instructions to get a copy of the project up and running on your local machine.

## Prerequisites

Python 3.12.0 (LTS)

Node 20.10.0 (LTS)

Neo4j 4.4.0

## Installing Node.js with nvm

[nvm](https://github.com/nvm-sh/nvm) (Node Version Manager) allows you to easily manage multiple Node.js versions. Follow the steps below to install Node.js version 20.10.0 (LTS) using `nvm`:

1. **Install nvm:**

   - Follow the instructions on the [nvm GitHub repository](https://github.com/nvm-sh/nvm#install--update-script) to install `nvm`.

2. **Install Node.js:**

   - Open a new terminal.

   - Run the following command to install Node.js version 20.10.0:

    ```bash
        nvm install 20.10.0
    ```

3. **Use Node.js:**

   - Set Node.js version 20.10.0 as the default for your projects:

    ```bash
        nvm use 20.10.0
    ```

   - Verify the installation:

    ```bash
        node -v
        npm -v
    ```

## Installing Python with pyenv

[pyenv](https://github.com/pyenv/pyenv) allows you to easily install and manage multiple Python versions. Follow the steps below to install Python version 3.12.0 (LTS) using `pyenv`:

1. **Install pyenv:**

   - Follow the instructions on the [pyenv GitHub repository](https://github.com/pyenv/pyenv#installation) to install `pyenv`.

2. **Install Python:**

   - Open a new terminal.

   - Run the following command to install Python version 3.12.0:

    ```bash
        pyenv install 3.12.0
    ```

3. **Use Python:**

   - Set Python version 3.12.0 as the default for your projects:

    ```bash
        pyenv global 3.12.0 # or pyenv local 3.12.0 in the projects main directory
    ```

   - Verify the local configuration by checking the `.python-version` file:

    ```bash
        cat .python-version
    ```

   - Verify the installation:

    ```bash
        python --version
        pip --version
    ```

## Setup the Project

1. **Setup Python Pip Packages:**
    
    Install the required packages using the following command:

    ```bash
        cd backend
        pip install -r requirements.txt
    ```

2. **Setup NodeJS:**

    ```bash
        cd frontend
        npm install # install package.json dependencies
    ```

## Start the Project

1. **Start the database:**

    ```bash
        sudo neo4j start # start neo4j
    ```

2. **Start backend:**

    ```bash
        cd backend
        python app.py # start python backend
    ```

3. **Start frontend:**

    ```bash
        cd frontend
        npm start # start nodejs frontend
    ```