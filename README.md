# sf-sync

`sf-sync` is a Spring Boot integration project that synchronizes Salesforce data into a local MySQL database using two integration patterns:

- Salesforce Pub/Sub API over gRPC for near real-time Account CDC events
- Salesforce Bulk API 2.0 with Spring Batch for Case backfill/scheduled sync

It demonstrates a hybrid sync design: near real-time CDC processing for frequently changing records and batch ingestion for larger backfill-style workloads.

## Overview

The application listens to Salesforce Change Data Capture events for Account changes and applies those changes to a local database. It also supports Bulk API based synchronization for Case records, where Salesforce query results are downloaded as CSV and processed through a Spring Batch job.

High-level flow:

```text
Salesforce CDC
    -> gRPC Pub/Sub stream
    -> Avro payload decoding
    -> Account event handler
    -> MySQL upsert / soft delete
    -> Error log or RabbitMQ DLQ when processing fails

Salesforce Bulk API
    -> Submit SOQL query job
    -> Poll job status
    -> Download CSV results
    -> Spring Batch reader/processor/writer
    -> MySQL insert/update
```

## Features

- Real-time Account sync using Salesforce Pub/Sub API and gRPC streaming
- Avro payload decoding for Salesforce CDC events
- Create, update, and delete handling for Account change events
- Bounded retry for transient gRPC `UNAVAILABLE` failures
- Scheduled stream restart when the Salesforce stream closes unexpectedly
- Database retry handling for transient connection/transaction failures
- Error logging into an `error_log` table for non-retryable failures
- RabbitMQ dead-letter queue path for events that cannot be safely processed
- Bulk API query job submission and polling for Case sync
- Spring Batch based CSV processing for bulk-imported Salesforce records
- MySQL persistence with JPA repositories and JDBC batch writers

## Tech stack

- Java
- Spring Boot
- Spring Data JPA
- Spring Batch
- Spring Retry
- Spring AMQP / RabbitMQ
- gRPC
- Salesforce Pub/Sub API
- Salesforce Bulk API 2.0
- Apache Avro
- MySQL
- Gradle

## Error-handling approach

The project separates failures into different categories instead of treating every exception the same way.

For gRPC streaming, transient `UNAVAILABLE` failures are retried with a bounded restart flow. Other gRPC failures are logged because they usually need configuration, authentication, permission, or topic-name investigation.

For CDC event processing, malformed or unprocessable events are not allowed to stop the full stream. They are routed to an error path so the remaining valid events can continue.

For database writes, transient connection and transaction failures are retried. Data integrity failures are handled separately because retrying the same invalid data usually does not fix the problem.

For batch sync, Salesforce Bulk API job status is checked before downloading results. CSV results are processed through a Spring Batch reader, processor, and writer so large result sets can be handled in chunks.

## Current scope

Implemented or in progress:

- Account CDC sync through Salesforce Pub/Sub API
- Account create/update/delete mapping into local database records
- gRPC retry/restart handling for transient stream failures
- RabbitMQ DLQ path for failed CDC processing
- Case Bulk API query flow and Spring Batch job structure

Planned improvements:

- Add stronger tests for CDC event handling, retry behavior, and batch processing
- Add a dedicated test profile so the project can run tests without a local MySQL instance
- Improve Bulk API failure handling for failed, aborted, or timed-out Salesforce jobs
- Add DLQ replay tooling for failed Salesforce record IDs
- Reduce debug logging and remove old experimental code paths
- Add Docker Compose for MySQL and RabbitMQ local setup

## Local setup

Prerequisites:

- Java 25 or the Java version configured in `build.gradle`
- MySQL
- RabbitMQ
- Salesforce org with CDC enabled
- Salesforce connected app credentials

Create a local configuration file from the example properties file:

```bash
cp src/main/resources/application.proprties.example src/main/resources/application.properties
```

Then update the local values for:

- Salesforce login URL
- Salesforce client ID and client secret
- Salesforce org ID
- MySQL connection details
- RabbitMQ connection details
- Salesforce gRPC channel target

Do not commit `src/main/resources/application.properties`. It is intentionally ignored because it contains local credentials.

## Running locally

Start MySQL and RabbitMQ first, then run:

```bash
./gradlew bootRun
```

Run tests with:

```bash
./gradlew test
```

## Technical highlights

- Built a Spring Boot Salesforce sync service using gRPC-based CDC streaming and Bulk API batch ingestion.
- Implemented Account create/update/delete synchronization from Salesforce Change Data Capture events into MySQL.
- Added bounded retry and restart handling for transient gRPC stream failures.
- Designed failure handling with database error logging and RabbitMQ dead-letter routing for failed events.
- Built Spring Batch jobs to process Salesforce Bulk API CSV results into local database tables.

## Project status

The main integration design is in place, with CDC processing, retry handling, DLQ routing, and batch processing represented in code. Remaining work is focused on cleanup, tests, and hardening the Bulk API failure path.
