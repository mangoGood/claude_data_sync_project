package com.migration.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RetryUtil {
    private static final Logger logger = LoggerFactory.getLogger(RetryUtil.class);

    public static <T> T executeWithRetry(RetryableOperation<T> operation,
                                         int maxRetries,
                                         long initialDelayMs,
                                         long maxDelayMs) throws Exception {
        return executeWithRetry(operation, maxRetries, initialDelayMs, maxDelayMs, null);
    }

    public static <T> T executeWithRetry(RetryableOperation<T> operation,
                                         int maxRetries,
                                         long initialDelayMs,
                                         long maxDelayMs,
                                         ErrorHandler errorHandler) throws Exception {
        Exception lastException = null;
        long delay = initialDelayMs;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return operation.execute();
            } catch (Exception e) {
                lastException = e;

                if (attempt < maxRetries) {
                    if (errorHandler != null) {
                        errorHandler.handleError(e, attempt + 1, maxRetries);
                    } else {
                        logger.warn("Operation failed (attempt {}/{}), retrying in {} ms. Error: {}",
                            attempt + 1, maxRetries, delay, e.getMessage());
                    }

                    try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new Exception("Retry interrupted", ie);
                    }

                    delay = Math.min((long) (delay * 1.5), maxDelayMs);
                }
            }
        }

        logger.error("Operation failed after {} attempts", maxRetries + 1);
        throw lastException;
    }

    public static void executeWithRetry(RetryableVoidOperation operation,
                                        int maxRetries,
                                        long initialDelayMs,
                                        long maxDelayMs) throws Exception {
        executeWithRetry(() -> {
            operation.execute();
            return null;
        }, maxRetries, initialDelayMs, maxDelayMs);
    }

    @FunctionalInterface
    public interface RetryableOperation<T> {
        T execute() throws Exception;
    }

    @FunctionalInterface
    public interface RetryableVoidOperation {
        void execute() throws Exception;
    }

    @FunctionalInterface
    public interface ErrorHandler {
        void handleError(Exception e, int attempt, int maxRetries);
    }
}
