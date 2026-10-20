package dev.ingest.core;

/**
 * Unchecked exception thrown when a source connector encounters an unrecoverable error.
 */
public class ConnectorException extends RuntimeException {

    private final String reasonCode;
    private final boolean retryable;

    public ConnectorException(String reasonCode, String message, boolean retryable) {
        super(message);
        this.reasonCode = reasonCode;
        this.retryable = retryable;
    }

    public ConnectorException(String reasonCode, String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode;
        this.retryable = retryable;
    }

    public String reasonCode() {
        return reasonCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
