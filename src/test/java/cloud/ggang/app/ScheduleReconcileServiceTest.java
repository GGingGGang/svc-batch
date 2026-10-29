package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

class ScheduleReconcileServiceTest {
    @Test void allDayReminderUsesNineAmInScheduleTimezone() {
        JdbcTemplate jdbc=mock(JdbcTemplate.class);
        when(jdbc.queryForObject(contains("account_lifecycle_guard"),eq(Boolean.class),any(byte[].class)))
                .thenReturn(false);
        when(jdbc.query(anyString(),any(RowMapper.class),any(Object[].class))).thenReturn(List.of());
        ScheduleEventPayload event=new ScheduleEventPayload(UUID.randomUUID().toString(),UUID.randomUUID().toString(),
                "title",null,null,true,"manual","confirmed",1L,List.of(new ReminderPayload(30,"push")),
                Instant.parse("2026-10-01T00:00:00Z"),"Asia/Seoul",LocalDate.of(2026,10,30),LocalDate.of(2026,10,31));

        new ScheduleReconcileService(jdbc).upsert(event,false);

        ArgumentCaptor<Object[]> values=ArgumentCaptor.forClass(Object[].class);
        verify(jdbc).update(contains("INSERT INTO reminder_dispatch"),values.capture());
        assertThat(values.getValue()[6]).isEqualTo(LocalDateTime.of(2026,10,30,0,0));
        assertThat(values.getValue()[7]).isEqualTo(LocalDateTime.of(2026,10,29,23,30));
    }
}
