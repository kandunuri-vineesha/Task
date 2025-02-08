# Real-Time Data Synchronization System for Heterogeneous Databases

## Overview

This repository contains a Java-based Spring Boot application that provides real-time data synchronization across multiple heterogeneous databases (MySQL, PostgreSQL, MongoDB, etc.). The system is designed for minimal latency, eventual consistency, and robust handling of network failures. It leverages Change Data Capture (CDC) mechanisms to efficiently track and propagate data changes in a bidirectional manner, with built-in conflict resolution strategies.

## Features

*   **Real-Time Data Synchronization:** Near-instantaneous data updates across databases.
*   **Heterogeneous Database Support:** Supports synchronization between MySQL, PostgreSQL, and MongoDB (extensible to other databases).
*   **Bidirectional Replication:**  Changes in *any* database are propagated to all other databases.
*   **Conflict Resolution:** Implements customizable conflict resolution strategies, including:
    *   Last Write Wins (LWW) based on timestamp.
    *   (Placeholder for future merge logic implementations).
*   **Dynamic Schema Change Handling:** Automatically detects and adapts to schema changes (e.g., adding columns, renaming tables) using Debezium's schema history.
*   **Efficient Change Data Capture (CDC):**  Utilizes log-based CDC via Debezium for minimal performance impact on source databases.
*   **Network Failure Handling and Automatic Recovery:**  Resilient to network outages with automatic recovery and retry mechanisms, ensuring eventual consistency.
*   **Spring Boot and Spring Batch:** Leverages the power and scalability of Spring Boot and Spring Batch for robust data processing.
*   **Monitoring and Health Checks:**  Includes Actuator endpoints for monitoring application health and database connectivity.

## Architecture

The system follows a microservices-inspired architecture with the following core components:

*   **CDC Connectors (Debezium):** Capture changes from each source database and publish them to Kafka topics.
*   **Apache Kafka:**  Acts as a central, fault-tolerant message bus for distributing change events.
*   **Data Synchronization Processor:**  A Spring Boot application with a Spring Batch job that:
    *   Consumes change events from Kafka topics.
    *   Transforms the data into a common format.
    *   Applies conflict resolution strategies.
    *   Routes and writes the changes to the appropriate target databases.
*   **Database Adapters:**  Provide database-specific logic for applying changes (inserts, updates, deletes) to the target databases.
