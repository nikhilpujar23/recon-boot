package com.recon.api.config;

import com.recon.api.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleBadRequest(IllegalArgumentException ex,
                                                          HttpServletRequest req) {
        String requestId = (String) req.getAttribute("requestId");
        log.warn("Bad request: {}", ex.getMessage());
        return ResponseEntity.status(400)
                .body(ErrorResponse.of("BAD_REQUEST", ex.getMessage(), requestId));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, HttpServletRequest req) {
        String requestId = (String) req.getAttribute("requestId");
        log.error("Unhandled exception", ex);
        return ResponseEntity.status(500)
                .body(ErrorResponse.of("INTERNAL_ERROR", "An unexpected error occurred", requestId));
    }
}
