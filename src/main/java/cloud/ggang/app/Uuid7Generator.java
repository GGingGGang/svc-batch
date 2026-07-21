package cloud.ggang.app;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

// UUIDv7 (RFC 9562) — 48bit ms epoch + 4bit version + 12bit rand_a + 2bit variant + 62bit rand_b.
// reminder_dispatch.id 생성용 — 시간 정렬성 확보.
public final class Uuid7Generator {

    private static final SecureRandom RANDOM = new SecureRandom();

    private Uuid7Generator() {}

    public static UUID generate() {
        byte[] value = new byte[16];
        RANDOM.nextBytes(value);

        long timestamp = Instant.now().toEpochMilli();
        value[0] = (byte) (timestamp >>> 40);
        value[1] = (byte) (timestamp >>> 32);
        value[2] = (byte) (timestamp >>> 24);
        value[3] = (byte) (timestamp >>> 16);
        value[4] = (byte) (timestamp >>> 8);
        value[5] = (byte) timestamp;

        value[6] = (byte) (0x70 | (value[6] & 0x0F)); // version 7
        value[8] = (byte) (0x80 | (value[8] & 0x3F)); // variant 10

        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (value[i] & 0xFF);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (value[i] & 0xFF);
        }
        return new UUID(msb, lsb);
    }
}
