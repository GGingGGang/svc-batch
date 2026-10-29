package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;

class ReminderHistoryControllerTest {

    private static final String TOKEN = "01234567890123456789012345678901";
    private static final String USER_ID = "25f9b7d0-f0ee-48f3-a839-a33547630e31";

    @Test
    void enforcesInternalAuthAndMaximumWindowBeforeQuerying() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ReminderHistoryController controller = controller(jdbc);

        assertThatThrownBy(() -> controller.history(USER_ID, Instant.EPOCH, Instant.EPOCH.plusSeconds(31L * 86400),
                100, "Bearer " + TOKEN))
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> controller.history(USER_ID, Instant.EPOCH, Instant.EPOCH, 100, "Bearer wrong"))
                .isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(jdbc);
    }

    @Test
    void limitsAndScopesHistoryQueryToRequestedUserAndWindow() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ReminderHistoryController controller = controller(jdbc);
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to = from.plusSeconds(30L * 86400);
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());

        ReminderHistoryController.ReminderHistoryResponse response = controller.history(
                USER_ID, from, to, 100, "Bearer " + TOKEN);

        assertThat(response.reminders()).isEmpty();
        verify(jdbc).query(org.mockito.ArgumentMatchers.contains("WHERE d.user_id=? AND d.remind_at>=?"),
                any(RowMapper.class), eq(UuidBytes.toBytes(java.util.UUID.fromString(USER_ID))),
                eq(java.sql.Timestamp.from(from)), eq(java.sql.Timestamp.from(to)), eq(100));
    }

    private ReminderHistoryController controller(JdbcTemplate jdbc) {
        ReminderHistoryController controller = new ReminderHistoryController(jdbc);
        ReflectionTestUtils.setField(controller, "internalToken", TOKEN);
        return controller;
    }
}
