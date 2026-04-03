# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A provider-agnostic Bonita connector for sending and receiving JMS messages. Uses JNDI for `ConnectionFactory` and `Destination` lookups, supporting any JMS provider (ActiveMQ, IBM MQ, RabbitMQ JMS, etc.). Targets Bonita 2025.2+ (runtime BOM 10.4.5), Java 17, Jakarta JMS 3.1.

## Build & Test Commands

```bash
# Build and package (produces target/bonita-connector-jms-*.zip for Bonita Studio import)
./mvnw clean package

# Full build including tests
./mvnw clean verify

# Run all tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=JmsConnectorTest

# Run a single test method
./mvnw test -Dtest=JmsConnectorTest#should_fail_when_uri_is_missing
```

## Architecture

**Single module, two key source files:**

- `src/main/java/org/bonitasoft/connectors/JmsConnector.java` — the entire connector logic (~360 lines). Extends `AbstractConnector` from the Bonita engine.
- `src/test/java/org/bonitasoft/connectors/JmsConnectorTest.java` — unit tests using JUnit 5, AssertJ, and Mockito.

**Connector lifecycle** (standard Bonita pattern):
1. `validateInputParameters()` — validates required inputs before execution
2. `connect()` — creates JNDI `InitialContext`, looks up `ConnectionFactory`, creates and starts JMS `Connection`
3. `executeBusinessLogic()` — dispatches to `executeSend()` or `executeReceive()` based on `operation` input
4. `disconnect()` — closes JMS connection and JNDI context

**Two operations:**
- `SEND`: creates a `TextMessage`, applies optional JMS properties (`List<List<String>>`), sends to destination, returns `messageId`
- `RECEIVE`: uses an optional JMS selector, waits up to `receiveTimeout` ms (default 5000), returns `statusCode` (0=success, 1=timeout, -1=error), `receivedMessage`, and `receivedProperties`

**All parameter names** are defined as static String constants at the top of `JmsConnector.java`.

## Descriptor Files

`src/main/resources-filtered/` contains Maven-filtered XML descriptors:
- `bonita-connector-jms.def` — connector definition (4 UI pages: connection, operation, message, receive)
- `bonita-connector-jms.impl` — links definition to the implementation class

Maven property substitution (e.g., `${connector-definition-id}`) runs at build time via resource filtering. Do not edit `.def`/`.impl` files without understanding which values are literals vs. filtered placeholders.

## Packaging

`src/assembly/assembly.xml` produces a ZIP that includes the connector JAR and a `classpath/` directory with runtime dependencies (extracted via `src/script/dependencies-as-var.groovy`). This ZIP is what gets imported into Bonita Studio.

## Dependencies Scope Notes

- `bonita-common` and `jakarta.jms-api` are **provided** scope — they are supplied by the Bonita runtime and must not be bundled.
- Test dependencies (JUnit 5, AssertJ, Mockito, Logback) are **test** scope only.