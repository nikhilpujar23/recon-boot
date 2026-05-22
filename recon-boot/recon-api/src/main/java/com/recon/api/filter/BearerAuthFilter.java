package com.recon.api.filter;

import com.recon.common.config.AppConfig;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Order(1)
public class BearerAuthFilter implements Filter {

    private final String expectedToken;

    public BearerAuthFilter(AppConfig config) {
        this.expectedToken = "Bearer " + config.api().bearerToken();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest req  = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;

        String path = req.getRequestURI();
        // Public endpoints
        if (path.equals("/healthz") || path.startsWith("/actuator") || path.startsWith("/metrics")) {
            chain.doFilter(request, response);
            return;
        }

        String auth = req.getHeader("Authorization");
        if (auth == null || !auth.equals(expectedToken)) {
            res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            res.getWriter().write("{\"error\":{\"code\":\"UNAUTHORIZED\",\"message\":\"Invalid or missing token\"}}");
            return;
        }
        chain.doFilter(request, response);
    }
}
