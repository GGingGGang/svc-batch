package cloud.ggang.app;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HttpTracingFilter extends OncePerRequestFilter {

    private final Tracer tracer;
    private final io.opentelemetry.api.OpenTelemetry openTelemetry;

    public HttpTracingFilter(Tracer tracer, io.opentelemetry.api.OpenTelemetry openTelemetry) {
        this.tracer = tracer;
        this.openTelemetry = openTelemetry;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain chain) throws ServletException, IOException {
        Context parent = openTelemetry.getPropagators().getTextMapPropagator().extract(
                Context.current(), request, new io.opentelemetry.context.propagation.TextMapGetter<HttpServletRequest>() {
                    @Override
                    public Iterable<String> keys(HttpServletRequest carrier) {
                        return java.util.Collections.list(carrier.getHeaderNames());
                    }

                    @Override
                    public String get(HttpServletRequest carrier, String key) {
                        return carrier.getHeader(key);
                    }
                });
        Span span = tracer.spanBuilder("HTTP " + request.getMethod())
                .setParent(parent)
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        try (Scope ignored = span.makeCurrent()) {
            span.setAttribute("http.request.method", request.getMethod());
            chain.doFilter(request, response);
            span.setAttribute("http.response.status_code", response.getStatus());
            if (response.getStatus() >= 500) {
                span.setStatus(StatusCode.ERROR);
            }
        } catch (ServletException | IOException | RuntimeException ex) {
            span.setStatus(StatusCode.ERROR);
            throw ex;
        } finally {
            span.end();
        }
    }
}
