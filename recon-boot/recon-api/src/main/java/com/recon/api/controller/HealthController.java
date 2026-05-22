package com.recon.api.controller;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class HealthController {

    private final JdbcClient jdbc;

    public HealthController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/healthz")
    public Map<String, Object> health() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("status", "ok");
        status.put("db", checkDb());
        return status;
    }

    private String checkDb() {
        try {
            jdbc.sql("SELECT 1").query(Integer.class).single();
            return "ok";
        } catch (Exception e) {
            return "error: " + e.getMessage();
        }
    }
}
