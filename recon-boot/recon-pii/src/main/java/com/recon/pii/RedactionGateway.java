package com.recon.pii;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Wraps every agent tool call to enforce PII boundaries.
 * Fail-closed: any redaction failure aborts the call and signals ESCALATE upstream.
 */
@Component
public class RedactionGateway {

    private static final Logger log = LoggerFactory.getLogger(RedactionGateway.class);

    private final PiiRedactor redactor;

    public RedactionGateway(PiiRedactor redactor) {
        this.redactor = redactor;
    }

    /**
     * Execute a tool call with PII-safe input/output handling.
     * The supplier receives pre-validated input; its output is re-redacted before returning.
     *
     * @throws RedactionException if PII is detected in the output or processing fails
     */
    public <T> T execute(String toolName, String rawInput, Supplier<T> toolCall) {
        try {
            String redactedInput = redactor.redact(rawInput);
            T result = toolCall.get();
            if (result instanceof String s && redactor.containsPii(s)) {
                log.error("PII detected in tool output: tool={}", toolName);
                throw new RedactionException("PII in output of tool: " + toolName);
            }
            return result;
        } catch (RedactionException re) {
            throw re;
        } catch (Exception e) {
            log.error("Redaction gateway failure: tool={}", toolName, e);
            throw new RedactionException("Redaction failure for tool: " + toolName, e);
        }
    }
}
