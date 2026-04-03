package org.bonitasoft.connectors;

import org.bonitasoft.engine.connector.ConnectorValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JmsConnectorTest {

    JmsConnector connector;

    @BeforeEach
    void setUp() {
        connector = new JmsConnector();
    }

    // ── Helper to build a valid parameter map ────────────────────────────

    private Map<String, Object> validSendParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put(JmsConnector.INPUT_URI, "tcp://localhost:61616");
        params.put(JmsConnector.INPUT_QUEUE_NAME, "test.queue");
        params.put(JmsConnector.INPUT_OPERATION, "SEND");
        params.put(JmsConnector.INPUT_MESSAGE, "Hello JMS");
        params.put(JmsConnector.INPUT_ANONYMOUS, true);
        return params;
    }

    private Map<String, Object> validReceiveParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put(JmsConnector.INPUT_URI, "tcp://localhost:61616");
        params.put(JmsConnector.INPUT_QUEUE_NAME, "test.queue");
        params.put(JmsConnector.INPUT_OPERATION, "RECEIVE");
        params.put(JmsConnector.INPUT_ANONYMOUS, true);
        params.put(JmsConnector.INPUT_RECEIVE_TIMEOUT, 3000L);
        return params;
    }

    // ── Validation: missing mandatory inputs ─────────────────────────────

    @Nested
    @DisplayName("Validation — mandatory inputs")
    class MandatoryInputTests {

        @Test
        @DisplayName("should fail when URI is missing")
        void should_fail_when_uri_is_missing() {
            Map<String, Object> params = validSendParameters();
            params.remove(JmsConnector.INPUT_URI);
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should fail when URI is empty")
        void should_fail_when_uri_is_empty() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_URI, "");
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should fail when queue name is missing")
        void should_fail_when_queue_name_is_missing() {
            Map<String, Object> params = validSendParameters();
            params.remove(JmsConnector.INPUT_QUEUE_NAME);
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should fail when operation is missing")
        void should_fail_when_operation_is_missing() {
            Map<String, Object> params = validSendParameters();
            params.remove(JmsConnector.INPUT_OPERATION);
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }
    }

    // ── Validation: operation values ─────────────────────────────────────

    @Nested
    @DisplayName("Validation — operation input")
    class OperationValidationTests {

        @Test
        @DisplayName("should fail for invalid operation value")
        void should_fail_for_invalid_operation() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_OPERATION, "DELETE");
            connector.setInputParameters(params);

            assertThatThrownBy(() -> connector.validateInputParameters())
                    .isInstanceOf(ConnectorValidationException.class)
                    .hasMessageContaining("SEND")
                    .hasMessageContaining("RECEIVE");
        }

        @Test
        @DisplayName("should accept SEND operation (case insensitive)")
        void should_accept_send_operation() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_OPERATION, "send");
            connector.setInputParameters(params);

            assertDoesNotThrow(() -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should accept RECEIVE operation")
        void should_accept_receive_operation() {
            Map<String, Object> params = validReceiveParameters();
            connector.setInputParameters(params);

            assertDoesNotThrow(() -> connector.validateInputParameters());
        }
    }

    // ── Validation: SEND-specific ────────────────────────────────────────

    @Nested
    @DisplayName("Validation — SEND operation")
    class SendValidationTests {

        @Test
        @DisplayName("should fail when message is missing for SEND")
        void should_fail_when_message_missing_for_send() {
            Map<String, Object> params = validSendParameters();
            params.remove(JmsConnector.INPUT_MESSAGE);
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should fail when message is empty for SEND")
        void should_fail_when_message_empty_for_send() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_MESSAGE, "");
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should pass validation with valid SEND parameters")
        void should_pass_with_valid_send_params() {
            connector.setInputParameters(validSendParameters());
            assertDoesNotThrow(() -> connector.validateInputParameters());
        }
    }

    // ── Validation: credentials ──────────────────────────────────────────

    @Nested
    @DisplayName("Validation — credentials")
    class CredentialValidationTests {

        @Test
        @DisplayName("should require username/password when not anonymous")
        void should_require_credentials_when_not_anonymous() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_ANONYMOUS, false);
            // No username/password
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should pass when anonymous is null and credentials provided")
        void should_pass_with_credentials_when_anonymous_null() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_ANONYMOUS, null);
            params.put(JmsConnector.INPUT_USERNAME, "admin");
            params.put(JmsConnector.INPUT_PASSWORD, "secret");
            connector.setInputParameters(params);

            assertDoesNotThrow(() -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should pass when anonymous is true (no credentials needed)")
        void should_pass_when_anonymous_true() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_ANONYMOUS, true);
            connector.setInputParameters(params);

            assertDoesNotThrow(() -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should pass with credentials when not anonymous")
        void should_pass_with_credentials_when_not_anonymous() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_ANONYMOUS, false);
            params.put(JmsConnector.INPUT_USERNAME, "admin");
            params.put(JmsConnector.INPUT_PASSWORD, "secret");
            connector.setInputParameters(params);

            assertDoesNotThrow(() -> connector.validateInputParameters());
        }
    }

    // ── Validation: RECEIVE-specific ─────────────────────────────────────

    @Nested
    @DisplayName("Validation — RECEIVE operation")
    class ReceiveValidationTests {

        @Test
        @DisplayName("should pass without message for RECEIVE")
        void should_pass_without_message_for_receive() {
            Map<String, Object> params = validReceiveParameters();
            connector.setInputParameters(params);

            assertDoesNotThrow(() -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should pass with message selector for RECEIVE")
        void should_pass_with_selector_for_receive() {
            Map<String, Object> params = validReceiveParameters();
            params.put(JmsConnector.INPUT_MESSAGE_SELECTOR, "correlationId = 'abc123'");
            connector.setInputParameters(params);

            assertDoesNotThrow(() -> connector.validateInputParameters());
        }
    }

    // ── Input type validation ────────────────────────────────────────────

    @Nested
    @DisplayName("Validation — input types")
    class InputTypeTests {

        @Test
        @DisplayName("should fail when URI is not a String")
        void should_fail_when_uri_not_string() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_URI, 12345);
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }

        @Test
        @DisplayName("should fail when queue name is not a String")
        void should_fail_when_queue_name_not_string() {
            Map<String, Object> params = validSendParameters();
            params.put(JmsConnector.INPUT_QUEUE_NAME, 42);
            connector.setInputParameters(params);

            assertThrows(ConnectorValidationException.class,
                    () -> connector.validateInputParameters());
        }
    }

    // ── Constants verification ───────────────────────────────────────────

    @Test
    @DisplayName("operation constants should be SEND and RECEIVE")
    void should_have_correct_operation_constants() {
        assertThat(JmsConnector.OPERATION_SEND).isEqualTo("SEND");
        assertThat(JmsConnector.OPERATION_RECEIVE).isEqualTo("RECEIVE");
    }
}
