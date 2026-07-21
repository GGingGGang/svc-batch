package cloud.ggang.app;

import java.nio.ByteBuffer;
import java.util.UUID;

// UUID <-> BINARY(16) 변환 (RFC4122 표준 16바이트 배치, most/least significant bits 순서).
public final class UuidBytes {

    private UuidBytes() {}

    public static byte[] toBytes(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

    public static UUID fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }
}
