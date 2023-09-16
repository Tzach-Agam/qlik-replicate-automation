"""This module contains configuration data for database endpoints (both sources and targets) in replicate software.

These dictionaries define the configuration settings for different database endpoints, including server details,
credentials, and other related information, and they will be used for creating, choosing or accessing  an endpoint on
Qlik Replicate software.

You can access these configurations by importing this module and using the defined variables."""


"""---------------------------------------- Sources ----------------------------------------"""

sqlserver_source_endpoint = {
    "name": "SQL_src",
    "description": "Endpoint for testing",
    "role": "source",
    "type": "SQL Server",
    "server": "tzachsqlserver19.qdinet",
    "username": "sa",
    "password": "nrWEL7zZwmXPZueb",
    "database": "tzach_src"
}

oracle_source_endpoint = {
    "name": "Oracle_src",
    "description": "Endpoint for testing",
    "role": "source",
    "type": "Oracle",
    "server": "tzachoracle19.qdinet",
    "username": "system",
    "password": "oracle"
}

mysql_source_endpoint = {
    "name": "MySQL_src",
    "description": "Endpoint for testing",
    "role": "source",
    "type": "MySQL",
    "server": "tzachmysql8.qdinet",
    "username": "root",
    "password": "Kofiko123"
}

postgres_source_endpoint = {
    "name": "Postgres_src",
    "description": "Endpoint for testing",
    "role": "source",
    "type": "PostgreSQL",
    "server": "tzachpostgres13.qdinet",
    "username": "postgres",
    "password": "postgres",
    "database": "postgres"
}


"""---------------------------------------- Targets ----------------------------------------"""

oracle_target_endpoint = {
    "name": "Oracle_trg",
    "description": "Endpoint for testing",
    "role": "target",
    "type": "oracle",
    "server": "tzachoracle19.qdinet/orcl",
    "username": "system",
    "password": "oracle"
}

sqlserver_target_endpoint = {
    "name": "SQL_trg",
    "description": "Endpoint for testing",
    "role": "target",
    "type": "SQL Server",
    "server": "tzachoracle19.qdinet",
    "username": "sa",
    "password": "nrWEL7zZwmXPZueb",
    "database": "tzach_trg"
}

s3_target_endpoint = {
    "name": "S3_trg",
    "description": "Endpoint for testing",
    "role": "target",
    "type": "S3",
    "bucket": "attibigdata",
    "access_key": "AKIAZKHSSZQOEE7O3DB3",
    "secret-key": "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"
}