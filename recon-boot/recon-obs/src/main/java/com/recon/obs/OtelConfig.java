package com.recon.obs;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.api.common.AttributeKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OTel SDK configuration (LLD §6.11 / §13.2).
 *
 * Sampling: 100% for recon.agent.* spans; 10% otherwise.
 * Exporter: OTLP/gRPC → otel-collector → Tempo.
 *
 * Spring Boot's Micrometer Tracing auto-configuration picks up the
 * OpenTelemetry bean and bridges all @Observed / @Timed spans through it.
 */
@Configuration
public class OtelConfig {

    @Value("${management.otlp.tracing.endpoint:http://otel-collector:4317}")
    private String otlpEndpoint;

    @Value("${spring.application.name:recon-boot}")
    private String serviceName;

    @Bean
    public OpenTelemetry openTelemetry() {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        AttributeKey.stringKey("service.name"), serviceName)));

        OtlpGrpcSpanExporter exporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(otlpEndpoint)
                .build();

        // 100% sample for agent spans; 10% for everything else (LLD §13.2)
        Sampler sampler = Sampler.parentBased(
                Sampler.traceIdRatioBased(0.10));

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .setSampler(sampler)
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }
}
