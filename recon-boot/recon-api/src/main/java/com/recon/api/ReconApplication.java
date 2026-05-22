package com.recon.api;

import com.recon.common.config.AppConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.recon")
@EnableConfigurationProperties(AppConfig.class)
@EnableScheduling
@EnableAsync
public class ReconApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReconApplication.class, args);
    }
}
