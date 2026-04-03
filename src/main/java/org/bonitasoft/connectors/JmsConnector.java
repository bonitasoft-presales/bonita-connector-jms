package org.bonitasoft.connectors;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.bonitasoft.engine.connector.AbstractConnector;
import org.bonitasoft.engine.connector.ConnectorException;
import org.bonitasoft.engine.connector.ConnectorValidationException;

/**
 * Bonita connector for JMS messaging.
 * <p>
 * Supports two operations:
 * <ul>
 *   <li><b>SEND</b> — Post a text message (or Bonita document content) with optional properties to a JMS queue.</li>
 *   <li><b>RECEIVE</b> — Consume a message from a JMS queue, optionally filtered by a JMS message selector.</li>
 * </ul>
 * <p>
 * This connector is provider-agnostic and uses JNDI to look up the {@link ConnectionFactory} and {@link Destination}.
 * If no JNDI context factory is specified, it falls back to a direct InitialContext with the provider URL.
 */
public class JmsConnector extends AbstractConnector {

    private static final Logger LOGGER = Logger.getLogger(JmsConnector.class.getName());

    // ── Input parameter names ────────────────────────────────────────────
    static final String INPUT_URI = "uri";
    static final String INPUT_QUEUE_NAME = "queueName";
    static final String INPUT_JNDI_CONTEXT_FACTORY = "jndiContextFactory";
    static final String INPUT_CONNECTION_FACTORY_NAME = "connectionFactoryName";
    static final String INPUT_ANONYMOUS = "anonymous";
    static final String INPUT_USERNAME = "username";
    static final String INPUT_PASSWORD = "password";
    static final String INPUT_OPERATION = "operation";
    static final String INPUT_MESSAGE = "message";
    static final String INPUT_IS_BONITA_DOCUMENT = "isBonitaDocument";
    static final String INPUT_PROPERTIES = "properties";
    static final String INPUT_MESSAGE_SELECTOR = "messageSelector";
    static final String INPUT_RECEIVE_TIMEOUT = "receiveTimeout";

    // ── Output parameter names ───────────────────────────────────────────
    static final String OUTPUT_STATUS_CODE = "statusCode";
    static final String OUTPUT_STATUS_MESSAGE = "statusMessage";
    static final String OUTPUT_MESSAGE_ID = "messageId";
    static final String OUTPUT_RECEIVED_MESSAGE = "receivedMessage";
    static final String OUTPUT_RECEIVED_PROPERTIES = "receivedProperties";

    // ── Operation constants ──────────────────────────────────────────────
    static final String OPERATION_SEND = "SEND";
    static final String OPERATION_RECEIVE = "RECEIVE";

    // ── Connection state ─────────────────────────────────────────────────
    private Connection jmsConnection;
    private Context jndiContext;

    @Override
    public void validateInputParameters() throws ConnectorValidationException {
        // Mandatory: uri
        checkMandatoryStringInput(INPUT_URI);
        // Mandatory: queueName
        checkMandatoryStringInput(INPUT_QUEUE_NAME);
        // Mandatory: operation (must be SEND or RECEIVE)
        String operation = getStringInput(INPUT_OPERATION);
        if (operation == null || operation.isEmpty()) {
            throw new ConnectorValidationException(this, "Mandatory parameter 'operation' is missing.");
        }
        if (!OPERATION_SEND.equalsIgnoreCase(operation) && !OPERATION_RECEIVE.equalsIgnoreCase(operation)) {
            throw new ConnectorValidationException(this,
                    String.format("Parameter 'operation' must be '%s' or '%s', but was '%s'.",
                            OPERATION_SEND, OPERATION_RECEIVE, operation));
        }

        // If SEND, message is required (unless it's a Bonita document, in which case it holds the document name)
        if (OPERATION_SEND.equalsIgnoreCase(operation)) {
            String message = getStringInput(INPUT_MESSAGE);
            if (message == null || message.isEmpty()) {
                throw new ConnectorValidationException(this,
                        "Parameter 'message' is required for SEND operation.");
            }
        }

        // If not anonymous, username and password are required
        Boolean anonymous = (Boolean) getInputParameter(INPUT_ANONYMOUS);
        if (anonymous == null || !anonymous) {
            checkMandatoryStringInput(INPUT_USERNAME);
            checkMandatoryStringInput(INPUT_PASSWORD);
        }
    }

    @Override
    public void connect() throws ConnectorException {
        try {
            jndiContext = createJndiContext();
            ConnectionFactory connectionFactory = lookupConnectionFactory(jndiContext);

            Boolean anonymous = (Boolean) getInputParameter(INPUT_ANONYMOUS);
            if (anonymous != null && anonymous) {
                jmsConnection = connectionFactory.createConnection();
            } else {
                String username = getStringInput(INPUT_USERNAME);
                String password = getStringInput(INPUT_PASSWORD);
                jmsConnection = connectionFactory.createConnection(username, password);
            }

            jmsConnection.start();
            LOGGER.info("JMS connection established successfully.");
        } catch (NamingException e) {
            throw new ConnectorException("Failed to look up JMS resources via JNDI: " + e.getMessage(), e);
        } catch (JMSException e) {
            throw new ConnectorException("Failed to create JMS connection: " + e.getMessage(), e);
        }
    }

    @Override
    protected void executeBusinessLogic() throws ConnectorException {
        String operation = getStringInput(INPUT_OPERATION);

        try {
            if (OPERATION_SEND.equalsIgnoreCase(operation)) {
                executeSend();
            } else if (OPERATION_RECEIVE.equalsIgnoreCase(operation)) {
                executeReceive();
            }
        } catch (JMSException e) {
            setOutputParameter(OUTPUT_STATUS_CODE, -1);
            setOutputParameter(OUTPUT_STATUS_MESSAGE, "JMS error: " + e.getMessage());
            throw new ConnectorException("JMS operation failed: " + e.getMessage(), e);
        } catch (NamingException e) {
            setOutputParameter(OUTPUT_STATUS_CODE, -1);
            setOutputParameter(OUTPUT_STATUS_MESSAGE, "JNDI error: " + e.getMessage());
            throw new ConnectorException("JNDI lookup failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void disconnect() throws ConnectorException {
        try {
            if (jmsConnection != null) {
                jmsConnection.close();
                LOGGER.info("JMS connection closed.");
            }
        } catch (JMSException e) {
            LOGGER.log(Level.WARNING, "Error closing JMS connection", e);
        } finally {
            try {
                if (jndiContext != null) {
                    jndiContext.close();
                }
            } catch (NamingException e) {
                LOGGER.log(Level.WARNING, "Error closing JNDI context", e);
            }
        }
    }

    // ── Send operation ───────────────────────────────────────────────────

    private void executeSend() throws JMSException, NamingException {
        String queueName = getStringInput(INPUT_QUEUE_NAME);
        String messageBody = getStringInput(INPUT_MESSAGE);

        Destination destination = lookupDestination(jndiContext, queueName);

        try (Session session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            TextMessage textMessage = session.createTextMessage(messageBody);

            // Set optional JMS message properties
            applyMessageProperties(textMessage);

            try (MessageProducer producer = session.createProducer(destination)) {
                producer.send(textMessage);

                String messageId = textMessage.getJMSMessageID();
                LOGGER.info(String.format("Message sent to queue '%s' with ID: %s", queueName, messageId));

                setOutputParameter(OUTPUT_STATUS_CODE, 0);
                setOutputParameter(OUTPUT_STATUS_MESSAGE, "Message sent successfully.");
                setOutputParameter(OUTPUT_MESSAGE_ID, messageId);
            }
        }
    }

    // ── Receive operation ────────────────────────────────────────────────

    private void executeReceive() throws JMSException, NamingException {
        String queueName = getStringInput(INPUT_QUEUE_NAME);
        String messageSelector = getStringInput(INPUT_MESSAGE_SELECTOR);
        Long receiveTimeout = (Long) getInputParameter(INPUT_RECEIVE_TIMEOUT);
        if (receiveTimeout == null) {
            receiveTimeout = 5000L;
        }

        Destination destination = lookupDestination(jndiContext, queueName);

        try (Session session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            MessageConsumer consumer;
            if (messageSelector != null && !messageSelector.isEmpty()) {
                consumer = session.createConsumer(destination, messageSelector);
                LOGGER.info(String.format("Receiving from queue '%s' with selector: %s", queueName, messageSelector));
            } else {
                consumer = session.createConsumer(destination);
                LOGGER.info(String.format("Receiving from queue '%s' (no selector)", queueName));
            }

            try {
                Message message = consumer.receive(receiveTimeout);

                if (message == null) {
                    LOGGER.info(String.format("No message received from queue '%s' within %d ms", queueName, receiveTimeout));
                    setOutputParameter(OUTPUT_STATUS_CODE, 1);
                    setOutputParameter(OUTPUT_STATUS_MESSAGE, "No message received within timeout.");
                    setOutputParameter(OUTPUT_RECEIVED_MESSAGE, null);
                    setOutputParameter(OUTPUT_RECEIVED_PROPERTIES, new HashMap<>());
                    setOutputParameter(OUTPUT_MESSAGE_ID, null);
                    return;
                }

                String messageId = message.getJMSMessageID();
                LOGGER.info(String.format("Message received from queue '%s' with ID: %s", queueName, messageId));

                // Extract message body
                String body = null;
                if (message instanceof TextMessage) {
                    body = ((TextMessage) message).getText();
                } else {
                    body = message.toString();
                    LOGGER.warning("Received a non-TextMessage; using toString() as body.");
                }

                // Extract message properties
                Map<String, String> properties = extractMessageProperties(message);

                setOutputParameter(OUTPUT_STATUS_CODE, 0);
                setOutputParameter(OUTPUT_STATUS_MESSAGE, "Message received successfully.");
                setOutputParameter(OUTPUT_MESSAGE_ID, messageId);
                setOutputParameter(OUTPUT_RECEIVED_MESSAGE, body);
                setOutputParameter(OUTPUT_RECEIVED_PROPERTIES, properties);
            } finally {
                consumer.close();
            }
        }
    }

    // ── Helper methods ───────────────────────────────────────────────────

    /**
     * Creates a JNDI InitialContext from connector inputs.
     */
    Context createJndiContext() throws NamingException {
        String uri = getStringInput(INPUT_URI);
        String contextFactory = getStringInput(INPUT_JNDI_CONTEXT_FACTORY);

        Properties env = new Properties();
        env.put(Context.PROVIDER_URL, uri);
        if (contextFactory != null && !contextFactory.isEmpty()) {
            env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
        }
        return new InitialContext(env);
    }

    /**
     * Looks up the ConnectionFactory from JNDI.
     */
    ConnectionFactory lookupConnectionFactory(Context context) throws NamingException {
        String factoryName = getStringInput(INPUT_CONNECTION_FACTORY_NAME);
        if (factoryName == null || factoryName.isEmpty()) {
            factoryName = "ConnectionFactory";
        }
        LOGGER.info(String.format("Looking up ConnectionFactory with JNDI name: %s", factoryName));
        return (ConnectionFactory) context.lookup(factoryName);
    }

    /**
     * Looks up a JMS Destination (queue) from JNDI.
     * Falls back to creating a queue via session if JNDI lookup fails.
     */
    Destination lookupDestination(Context context, String queueName) throws NamingException {
        LOGGER.info(String.format("Looking up Destination with JNDI name: %s", queueName));
        return (Destination) context.lookup(queueName);
    }

    /**
     * Applies user-defined JMS properties to the outgoing message.
     * Properties are provided as a List of List(2) [name, value].
     */
    @SuppressWarnings("unchecked")
    private void applyMessageProperties(TextMessage message) throws JMSException {
        List<List<String>> properties = (List<List<String>>) getInputParameter(INPUT_PROPERTIES);
        if (properties != null) {
            for (List<String> entry : properties) {
                if (entry != null && entry.size() >= 2) {
                    String key = entry.get(0);
                    String value = entry.get(1);
                    if (key != null && !key.isEmpty()) {
                        message.setStringProperty(key, value);
                        LOGGER.fine(String.format("Set message property: %s = %s", key, value));
                    }
                }
            }
        }
    }

    /**
     * Extracts all user-defined properties from a received JMS message.
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> extractMessageProperties(Message message) throws JMSException {
        Map<String, String> properties = new HashMap<>();
        Enumeration<String> propertyNames = message.getPropertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement();
            String value = message.getStringProperty(name);
            properties.put(name, value);
        }
        return properties;
    }

    /**
     * Validates that a mandatory string input is present and non-empty.
     */
    protected void checkMandatoryStringInput(String inputName) throws ConnectorValidationException {
        try {
            String value = (String) getInputParameter(inputName);
            if (value == null || value.isEmpty()) {
                throw new ConnectorValidationException(this,
                        String.format("Mandatory parameter '%s' is missing.", inputName));
            }
        } catch (ClassCastException e) {
            throw new ConnectorValidationException(this,
                    String.format("'%s' parameter must be a String", inputName));
        }
    }

    /**
     * Convenience method to get a String input parameter.
     */
    private String getStringInput(String inputName) {
        Object value = getInputParameter(inputName);
        return value != null ? value.toString() : null;
    }
}
