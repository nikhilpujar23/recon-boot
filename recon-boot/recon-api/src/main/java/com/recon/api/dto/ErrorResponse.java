package com.recon.api.dto;

public record ErrorResponse(ErrorBody error) {
    public record ErrorBody(String code, String message, String requestId) {}

    public static ErrorResponse of(String code, String message, String requestId) {
        return new ErrorResponse(new ErrorBody(code, message, requestId));
    }
}
