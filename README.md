# Bonita Connector JMS

A Bonita connector for sending and receiving messages from JMS queues. This connector is **provider-agnostic** and uses standard JNDI lookups to resolve the `ConnectionFactory` and `Destination`, making it compatible with any JMS provider (ActiveMQ, IBM MQ, RabbitMQ JMS, etc.).

## Compatibility

| Component       | Version                     |
|-----------------|-----------------------------|
| Bonita          | 2025.2+ (runtime BOM 10.4.5)|
| Java            | 17+                         |
| JMS API         | Jakarta JMS 3.1              |

## Features

- **SEND** — Post a text message to a JMS queue with optional properties
- **RECEIVE** — Consume a message from a JMS queue with optional message selector filtering
- **Bonita Document support** — Send document content from a Bonita process as a JMS message
- **Authenticated or anonymous** connections
- **Custom JMS properties** — Attach key-value properties to outgoing messages

## Connector Inputs

### Connection Configuration

| Input                    | Type      | Mandatory | Default              | Description                                     |
|--------------------------|-----------|-----------|----------------------|-------------------------------------------------|
| `uri`                    | String    | Yes       | `tcp://localhost:61616` | JNDI provider URL of the JMS broker            |
| `queueName`              | String    | Yes       |                      | JNDI name of the JMS queue                      |
| `jndiContextFactory`     | String    | No        |                      | Fully qualified JNDI InitialContextFactory class |
| `connectionFactoryName`  | String    | No        | `ConnectionFactory`  | JNDI name of the ConnectionFactory               |
| `anonymous`              | Boolean   | No        | `true`               | Whether the connection requires credentials      |
| `username`               | String    | No*       |                      | Username (required if not anonymous)             |
| `password`               | String    | No*       |                      | Password (required if not anonymous)             |

### Operation

| Input       | Type   | Mandatory | Default | Description              |
|-------------|--------|-----------|---------|--------------------------|
| `operation` | String | Yes       | `SEND`  | `SEND` or `RECEIVE`      |

### Send Options

| Input             | Type    | Mandatory | Description                                          |
|-------------------|---------|-----------|------------------------------------------------------|
| `message`         | String  | Yes*      | Text message or Bonita document name                 |
| `isBonitaDocument`| Boolean | No        | Whether `message` is a Bonita document name          |
| `properties`      | List    | No        | JMS message properties as key-value pairs            |

### Receive Options

| Input             | Type   | Mandatory | Default | Description                                  |
|-------------------|--------|-----------|---------|----------------------------------------------|
| `messageSelector` | String | No        |         | JMS message selector expression              |
| `receiveTimeout`  | Long   | No        | `5000`  | Max wait time in milliseconds                |

## Connector Outputs

| Output               | Type              | Description                                    |
|----------------------|-------------------|------------------------------------------------|
| `statusCode`         | Integer           | `0` = success, `1` = no message (timeout), `-1` = error |
| `statusMessage`      | String            | Descriptive status message                     |
| `messageId`          | String            | JMS message ID                                 |
| `receivedMessage`    | String            | Body of the received message (RECEIVE only)    |
| `receivedProperties` | Map<String,String> | Properties of the received message (RECEIVE only) |

## Build

```bash
./mvnw clean package
```

The built archive (ZIP) is located at `target/bonita-connector-jms-2.0.0-SNAPSHOT.zip`.

> **Note:** This project requires Java 17 and uses the `bonita-runtime-bom` (version 10.4.5) for Bonita dependency management, aligned with the official Bonita connector archetype for Bonita 2025.2.

Import this archive in Bonita Studio to use the connector in your processes.

## JMS Provider Setup

Since this connector uses JNDI, the JMS provider's client libraries must be available in the Bonita runtime classpath. For example:

- **Apache ActiveMQ Artemis**: Add `artemis-jms-client-all-*.jar` to Bonita's lib folder and use `org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory` as the JNDI Context Factory.
- **Apache ActiveMQ Classic**: Add `activemq-all-*.jar` and use `org.apache.activemq.jndi.ActiveMQInitialContextFactory`.
- **IBM MQ**: Add the IBM MQ JMS client JARs and configure the JNDI context factory accordingly.

## Example Usage in Bonita

### Sending a message

1. Add the JMS connector to a task or process
2. Configure the connection (broker URL, queue name)
3. Select **SEND** as the operation
4. Enter the message text and optional properties
5. Map the `statusCode` and `messageId` outputs to process variables

### Receiving a message

1. Add the JMS connector to a task or process
2. Configure the connection (broker URL, queue name)
3. Select **RECEIVE** as the operation
4. Optionally set a message selector (e.g., `correlationId = '${processInstanceId}'`)
5. Map the `receivedMessage` and `receivedProperties` outputs to process variables
