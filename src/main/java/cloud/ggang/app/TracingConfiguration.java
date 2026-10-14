package cloud.ggang.app;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracingConfiguration {

    private OpenTelemetrySdk sdk;

    @Bean
    public OpenTelemetry openTelemetry() {
        String serviceName = System.getenv().getOrDefault("OTEL_SERVICE_NAME", "batch");
        String exporter = System.getenv().getOrDefault("OTEL_TRACES_EXPORTER", "none");
        SdkTracerProviderBuilder provider = SdkTracerProvider.builder()
                .setResource(resource(serviceName, System.getenv().getOrDefault("OTEL_RESOURCE_ATTRIBUTES", "")))
                .setSampler(sampler(System.getenv().getOrDefault("OTEL_TRACES_SAMPLER", "parentbased_always_on"),
                        System.getenv().getOrDefault("OTEL_TRACES_SAMPLER_ARG", "1.0")));
        if ("otlp".equalsIgnoreCase(exporter)) {
            provider.addSpanProcessor(BatchSpanProcessor.builder(
                    OtlpGrpcSpanExporter.builder()
                            .setEndpoint(System.getenv().getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT",
                                    "http://localhost:4317"))
                            .setTimeout(Duration.ofSeconds(5))
                            .build()).build());
        }
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(provider.build())
                .setPropagators(ContextPropagators.create(io.opentelemetry.context.propagation.TextMapPropagator.composite(
                        W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                .build();
        return sdk;
    }

    @Bean
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("cloud.ggang.svc-batch");
    }

    @PreDestroy
    public void shutdown() {
        if (sdk != null) {
            sdk.close();
        }
    }

    static Resource resource(String serviceName, String attributes) {
        Resource resource = Resource.getDefault();
        for (String entry : attributes.split(",")) {
            String[] pair = entry.split("=", 2);
            if (pair.length == 2 && !pair[0].isBlank()) {
                resource = resource.toBuilder().put(pair[0].trim(), pair[1].trim()).build();
            }
        }
        return resource.toBuilder().put(AttributeKey.stringKey("service.name"), serviceName).build();
    }

    static Sampler sampler(String sampler, String argument) {
        if (sampler.equals("parentbased_always_off")) {
            return Sampler.parentBased(Sampler.alwaysOff());
        }
        if (sampler.equals("always_off")) {
            return Sampler.alwaysOff();
        }
        if (sampler.equals("always_on")) {
            return Sampler.alwaysOn();
        }
        if (sampler.equals("traceidratio") || sampler.equals("parentbased_traceidratio")) {
            double ratio;
            try {
                ratio = Double.parseDouble(argument);
            } catch (NumberFormatException ex) {
                return Sampler.parentBased(Sampler.alwaysOn());
            }
            if (!Double.isFinite(ratio) || ratio < 0 || ratio > 1) {
                return Sampler.parentBased(Sampler.alwaysOn());
            }
            Sampler ratioSampler = Sampler.traceIdRatioBased(ratio);
            return sampler.startsWith("parentbased_") ? Sampler.parentBased(ratioSampler) : ratioSampler;
        }
        return Sampler.parentBased(Sampler.alwaysOn());
    }
}
