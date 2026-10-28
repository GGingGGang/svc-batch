package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import org.junit.jupiter.api.Test;

class TracingConfigurationTest {

    @Test
    void serviceNameOverridesResourceAttribute() {
        assertThat(TracingConfiguration.resource("batch", "service.name=wrong,service.version=abc")
                        .getAttribute(AttributeKey.stringKey("service.name")))
                .isEqualTo("batch");
        assertThat(TracingConfiguration.resource("batch", "service.namespace=jobs")
                        .getAttribute(AttributeKey.stringKey("service.namespace")))
                .isEqualTo("jobs");
    }

    @Test
    void invalidSamplingRatioFallsBackToParentBasedAlwaysOn() {
        String expected = TracingConfiguration.sampler("parentbased_always_on", "1")
                .getDescription();
        assertThat(TracingConfiguration.sampler("traceidratio", "NaN").getDescription())
                .isEqualTo(expected);
        assertThat(TracingConfiguration.sampler("traceidratio", "1.1").getDescription())
                .isEqualTo(expected);
        assertThat(TracingConfiguration.sampler("parentbased_traceidratio", "oops").getDescription())
                .isEqualTo(expected);
    }

    @Test
    void samplingCanBeDisabled() {
        assertThat(TracingConfiguration.sampler("always_off", "").getDescription())
                .isEqualTo("AlwaysOffSampler");
        assertThat(TracingConfiguration.sampler("parentbased_always_off", "").getDescription())
                .contains("root:AlwaysOffSampler");
    }
}
