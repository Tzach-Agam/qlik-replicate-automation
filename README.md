# Qlik Replicate Automation <img width="30" src="https://spaces-cdn.clipsafari.com/7htufpvb30ul9aqplwr89hptxd32">

## Project Description
An automation project that serves as a comprehensive testing suite, aims to automate test scenarios and designed to ensure the reliability and functionality of Qlik Replicate software

**Objectives:<img width="20" src="https://www.pngitem.com/pimgs/m/341-3411784_objective-gif-png-png-download-goal-clipart-png.png">**

- Automate test cases to cover various aspects of Qlik Replicate, including critical functionality, user interfaces, and database interactions.
- Enhance software reliability by systematically running tests to identify and address issues promptly.
- Provide a framework for testing different databases and pages within the Qlik Replicate application.
- Work according to the Page Object Model (POM) in Selenium.
- Keep on maintainability, scalability, and collaboration by using different directories for different purposes.
- Enable users to configure and customize testing environemnt through a simple `config.ini` file.

## Table of Contents

1. [Introduction](#introduction)
2. [Directory Structure](#directory-structure)
3. [Installation and Setup](#installation-and-setup)
4. [Configuration](#configuration)
5. [Usage](#usage)
6. [Demo](#demo)
7. [Contact Information](#contact-information)

## Introduction

<div>Qlik Replicate is database replication software that allows users to accelerate database replication, big data-ingestion, and streaming data. It serves as a vital tool for organizations that need to efficiently replicate and integrate data across various platforms and databases.<div>
<div>By automating test cases for Qlik Replicate, this project aims to ensure the software functions correctly across a range of scenarios, thereby enhancing its reliability and robustness.<div>
<div> for more information on Replicate, you can check it's user guide: https://help.qlik.com/en-US/replicate/May2022/pdf/Replicate-Setup-and-User-Guide.pdf </div>
  
## Directory Structure

The project is structured for clarity and modularity:
- **browsers**: Contains a module for creating Selenium WebDriver instances for various browsers and modes.
- **replicate_pages**: Contains modules corresponding to different pages and user interfaces within the Qlik Replicate application. Each module focuses on interacting with a specific aspect of the software's UI.
- **databases**: Houses modules designed to interact with various relational databases (SQL Server, Oracle, MySQL and Postgres) and Amazon S3 storage. These are some of the endpoints that Qlik Replicate supports for data replication and integration. Each module encapsulates database-specific functionality .
- **tests**: This directory contains modules responsible for executing automated test cases. Each module corresponds to a particular database and leverages the modules in `replicate_pages` and `databases`.
- **configuration**: Provides a central location for managing configuration settings. The `config.ini` file within this directory allows users to customize their environments, such as specifying URLs, database credentials, and test data.
- **utilities**: Contains utility functions used throughout the project, promoting code reusability and maintainability.

## Installation and Setup

To get started with running automated tests for Qlik Replicate, you'll need to set up your environment:

- Install Qlik Replicate on-premise software
- Install drivers for the following databases: SQL Server + Oracle + Postgres + MySQL
- Acquire databases for the following drivers: SQL Server + Oracle + Postgres + MySQL
- Acquire an Amazon S3 stroage
- Ensure you have Python 3.x installed on your system.
- Install the necessary Selenium WebDriver (e.g., ChromeDriver) and add it to your system's PATH.
- Install the required Python packages using the following command:
  ```bash
  pip install -r requirements.txt

## Configuration

For configuring the test environment, utilize the `config.ini` file within the `configuration` directory. This file offers a straightforward way to customize your test scenarios. You can adjust settings like Qlik Replicate URLs, database credentials, and test data.

Here's a sample `config.ini` structure:

```ini
[Replicate]
replicate_url = https://qlikreplicate.example.com

[Database]
db_host = localhost
db_user = your_username
db_password = your_password
```

## Usage
Execute automated tests for Qlik Replicate using the following command:
```ini
python run_tests.py
```
You have the flexibility to specify additional options or arguments to target specific test modules or scenarios according to your testing needs.

## Demo
A short video demonstration of the automated test cases in action, which you can view here. This video offers a visual representation of how the tests interact with Qlik Replicate's user interface and database functionality.
The automated tests within this project are designed to rigorously validate Qlik Replicate's functionality across a range of use cases, ensuring its reliability and accuracy.


## Contact Information


