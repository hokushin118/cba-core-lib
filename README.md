# cba-core-lib Shared Library

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.9](https://img.shields.io/badge/Python-3.9-green.svg)](https://shields.io/)

This repository hosts a shared Python library intended for reuse across
multiple microservices within the application.

## Shared Library Purpose: Shared Utilities

The `cba-core-lib` is a shared Python library designed to provide reusable
functionality. Its primary function is to offer common utilities and components
that can be leveraged across various microservices.

**Design Principles:**

* **Reusability:** Focus on creating modular and reusable components.
* **Reliability:** Implement robust error handling and data validation.
* **Maintainability:** Adhere to clean code principles and best practices.
* **Testability:** Design components to be easily testable.

## Key Components and Libraries

This library is built using the following core technologies:

* **[Python 3.9](https://www.python.org/downloads/release/python-390/):**
    * The primary programming language used for development.
    * Chosen for its readability, extensive libraries, and strong community
      support.

**Additional Libraries and Tools:**

* **pytest:** For unit testing.
    * **pylint:** For linting.

**Rationale:**

* [Python 3.9](https://www.python.org/downloads/release/python-390/)
  provides a stable and modern environment for development.
* The additional libraries are selected based on their functionality and
  suitability for the project's requirements.

## Prerequisites

To develop and run this project, you'll need the following tools and software
installed:

**Required:**

* **Python 3.9:**
    * Download and install Python 3.9 from the official
      website: [Python 3.9 Downloads](https://www.python.org/downloads/release/python-390/)
    * Ensure
      that [Python 3.9 Downloads](https://www.python.org/downloads/release/python-390/)
      is added to your system's PATH environment variable.

**Optional (Recommended for Development):**

* **Integrated Development Environment (IDE):**
    * Choose an IDE for efficient development:
        * [PyCharm](https://www.jetbrains.com/pycharm) (Recommended for Python
          development)
        * [Visual Studio Code](https://code.visualstudio.com) (Highly versatile
          and extensible)

**Operating System Compatibility:**

* This project is designed to be cross-platform compatible.

**Important Notes:**

* Ensure that all required software is installed and configured correctly
  before proceeding with development.
* Using a virtual environment for Python development is strongly recommended to
  isolate project dependencies.

## Project Versioning

This project adheres to [Semantic Versioning 2.0.0](https://semver.org) for
managing releases.

**Version File:**

* The current project version is defined in the `VERSION` file, located in the
  root directory of the repository.
* This file contains a single line representing the version number.

**Versioning Scheme:**

* The version number follows the format `X.Y.Z`, where:
    * `X` (Major Version): Incremented when incompatible API changes are made.
    * `Y` (Minor Version): Incremented when new functionality is added in a
      backward-compatible manner.
    * `Z` (Patch Version): Incremented when backward-compatible bug fixes are
      made.

**Example:**

* `1.2.3` indicates major version 1, minor version 2, and patch version 3.

**Benefits of Semantic Versioning:**

* **Clarity:** Provides a clear indication of the type of changes included in
  each release.
* **Compatibility:** Helps users understand the potential impact of upgrading
  to a new version.
* **Automation:** Enables automated dependency management and release
  processes.

**Updating the Version:**

* When making changes to the project, update the `VERSION` file accordingly.
* Follow the [Semantic Versioning 2.0.0](https://semver.org) rules to determine
  which part of the version number to increment.

**Release Notes:**

* Each release should be accompanied by detailed release notes that describe
  the changes made.

## Local Development Setup

This section outlines the steps to set up and run the microservice in a local
development environment.

**1. Clone the Repository:**

* Clone the `cba-core-lib` repository to your local machine:

    ```bash
    git clone [https://github.com/hokushin118/cba-core-lib.git](https://github.com/hokushin118/cba-core-lib.git)
    ```

**2. Navigate to the Project Directory:**

* Change your current directory to the cloned repository:

    ```bash
    cd cba-core-lib
    ```

**3. Create and Activate a Virtual Environment:**

* Create a virtual environment to isolate project dependencies:

    ```bash
    python3.9 -m venv .venv
    ```

    * Note: Ensure you have Python 3.9 installed. Adjust the version if needed.

* Activate the virtual environment:

    ```bash
    source .venv/bin/activate  # On macOS/Linux
    .venv\Scripts\activate     # On Windows
    ```

**4. Install Dependencies:**

* Install the required Python packages using `pip`:

    ```bash
    python3.9 -m pip install --upgrade pip
    python3.9 -m pip install -r requirements.txt
    ```

**Important Notes:**

* Verify that your application's configuration is correctly set up for the
  local development environment.
* If you encounter any dependency issues, ensure that your virtual environment
  is activated and that you have the correct Python version.
* When you are finished, deactivate the virtual environment:

    ```bash
    deactivate
    ```

## Running Tests

### Introduction

The library utilizes unit tests to verify individual components and
integration tests to ensure system-wide functionality.

**1. Unit Tests: Verifying Individual Components**

* **Focus on Isolation:**
    * Unit tests are designed to isolate and examine the smallest testable
      parts of the microservice, typically individual functions, methods, or
      classes.
    * This isolation is achieved through techniques like mocking and stubbing,
      which replace external dependencies with controlled simulations.
* **Granular Validation:**
    * The primary goal is to ensure that each component behaves as expected in
      isolation. This allows to pinpoint bugs at the most granular level,
      making debugging significantly easier.
* **Speed and Efficiency:**
    * Unit tests are generally fast to execute, enabling rapid feedback during
      development. This promotes a test-driven development (TDD) approach,
      where tests are written before the actual code.
* **Benefits:**
    * Improved code quality and maintainability.
    * Early detection of bugs.
    * Facilitates refactoring by providing confidence in the code's behavior.
    * Enhanced code documentation through executable examples.

**2. Integration Tests: Ensuring System-Wide Functionality**

* **Focus on Interactions:**
    * Integration tests go beyond individual components and examine how
      different parts of the microservice interact with each other.
    * This includes testing the communication between modules, services,
      databases, and external APIs.
* **Benefits:**
    * Detection of integration issues that are not apparent in unit tests.
    * Validation of system-level functionality and performance.
    * Increased confidence in the microservice's overall stability.
    * Verifying that all parts of the system work together.

### Execution

To execute the library's tests, follow these steps:

1. **Run Unit Tests:**
    * Execute the microservice's unit tests using `pytest`. You can run a basic
      test execution or include coverage reporting.
    * **Basic Test Execution:**
      ```bash
      pytest -v tests/unit
      ```
        * `pytest`: Executes the pytest test runner.
        * `-v`: Enables verbose output.
        * `tests/unit`: Specifies the directory where pytest should discover
          and execute unit tests.
    * **Test Execution with Coverage Reporting:**
      ```bash
      pytest -v --cov=service --cov-report=term-missing --cov-branch tests/unit
      ```
        * `pytest`: Executes the pytest test runner.
        * `-v`: Enables verbose output.
        * `--cov=service`: Enables test coverage measurement for the `service`
          module.
        * `--cov-report=term-missing`: Enables detailed terminal output,
          showing lines not covered by tests.
        * `--cov-branch`: Enables branch coverage measurement in addition to
          line coverage.
        * `tests/unit`: Specifies the directory where pytest should discover
          and execute unit tests.

    * **Test Execution with Coverage Reporting and Minimum Threshold:**
      ```bash
      pytest -v --cov=service --cov-report=term-missing --cov-branch --cov-fail-under=80 tests/unit
      ```
        * `pytest`: Executes the pytest test runner.
        * `-v`: Enables verbose output.
        * `--cov=service`: Enables test coverage measurement for the `service`
          module.
        * `--cov-report=term-missing`: Enables detailed terminal output,
          showing lines not covered by tests.
        * `--cov-branch`: Enables branch coverage measurement in addition to
          line coverage.
        * `--cov-fail-under=80`: Fails the test run if overall coverage is
          below 80%.
        * `tests/unit`: Specifies the directory where pytest should discover
          and execute unit tests.

    * **Explanation of Coverage Options:**

        * `--cov=service`: Specifies the module to measure coverage for.
        * `--cov-report=term-missing`: Displays missing lines in the terminal
          report.
        * `--cov-branch`: Enables branch coverage measurement.
        * `--cov-fail-under=80`: Enforces a minimum coverage threshold of 80%.
        * `--cov-report=html`: Generates an HTML coverage report.
        * `--cov-report=xml`: Generates an XML coverage report.

2. **Run Integration Tests:**
    * Execute the microservice's integration tests using `pytest`.
   ```bash
   pytest -v --with-integration tests/integration
   ```
    * `pytest`: Executes the pytest test runner.
    * `-v`: Enables verbose output.
    * `--with-integration`: Enables the execution of integration tests.
    * `tests/integration`: Specifies the directory where pytest should
      discover and execute integration tests.

**Important Notes:**

* Verify that your application's configuration is set up correctly for the
  testing environment.
* Review the test output for any failures or errors.
* If you are using a different testing framework than pytest, update the
  testing command accordingly.
