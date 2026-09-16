# sf-sync

`sf-sync` is a Spring Boot integration project for synchronizing Salesforce data with a local MySQL database. It explores two complementary integration patterns:

- near-real-time Account change processing through the Salesforce Pub/Sub API
- Account and Case backfills through Salesforce Bulk API 2.0 and Spring Batch

The project combines event-driven synchronization for ongoing changes with chunk-based batch processing for larger datasets.

## Architecture

### Change Data Capture

```text
Salesforce Account CDC
    -> Pub/Sub API over gRPC
    -> Avro event decoding
    -> Account change handler
    -> MySQL create, update, or soft delete
    -> Database error log / RabbitMQ failure queue
```

The CDC implementation reads Salesforce event schemas, decodes Avro payloads, and routes events to an object-specific handler. Account create, update, and delete events are mapped to local persistence operations.

### Bulk synchronization

```text
Salesforce Bulk API 2.0
    -> Submit SOQL query job
    -> Poll job status asynchronously
    -> Retrieve query results
    -> Process results with Spring Batch reader, processor, and writer
    -> MySQL persistence
```

Bulk synchronization is started through the application startup runner. The Bulk API client submits SOQL query jobs, polls Salesforce until the job is complete, and passes the returned result to Spring Batch for chunk-based processing.

## Features

- Salesforce OAuth client-credentials authentication
- Salesforce Pub/Sub API integration over gRPC
- Avro decoding for Salesforce CDC payloads
- Account create, update, and delete event handling
- bounded restart attempts for transient gRPC `UNAVAILABLE` errors
- Salesforce Bulk API 2.0 query creation and asynchronous status polling
- Spring Batch CSV readers, processors, and JDBC batch writers
- Account and Case bulk-import job definitions
- MySQL persistence using Spring Data JPA and JDBC
- database error logging for failed records
- RabbitMQ routing for CDC records that require manual recovery

## Failure handling

The application separates failures by type:

- Transient gRPC availability failures use a bounded restart flow.
- Unexpected stream completion triggers a delayed restart attempt.
- Database connectivity and transaction failures can be retried.
- Data-integrity failures are recorded separately because retrying unchanged invalid data is unlikely to succeed.
- CDC records that cannot be safely processed can be routed to RabbitMQ for later manual review and replay.

## Technology stack

- Java 25
- Spring Boot 4
- Spring Data JPA
- Spring Batch
- Spring Retry
- Spring AMQP and RabbitMQ
- gRPC and Protocol Buffers
- Apache Avro
- Salesforce Pub/Sub API
- Salesforce Bulk API 2.0
- MySQL
- Gradle

## Local setup

### Prerequisites

- Java 25
- MySQL
- RabbitMQ
- a Salesforce org with Change Data Capture enabled
- a Salesforce External Client App configured for the client-credentials flow

Create the local configuration file:

```bash
cp src/main/resources/application.proprties.example src/main/resources/application.properties
```

Update it with your:

- Salesforce login URL
- Salesforce client ID and client secret
- Salesforce organization ID
- CDC topic names
- MySQL connection details
- RabbitMQ connection details
- Salesforce gRPC channel target

## Run the application

Start MySQL and RabbitMQ, and then run:

```bash
./gradlew bootRun
```

## Roadmap

- enable the CDC subscription from the application startup flow
- handle failed, aborted, and timed-out Salesforce Bulk API jobs explicitly
- verify and harden Account and Case batch persistence behavior
- add automated tests for CDC handling, retry behavior, and batch processing
- provide Docker Compose setup for MySQL and RabbitMQ
