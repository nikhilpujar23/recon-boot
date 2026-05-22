package com.recon.pii;

public class RedactionException extends RuntimeException {
    public RedactionException(String message) { super(message); }
    public RedactionException(String message, Throwable cause) { super(message, cause); }
}
