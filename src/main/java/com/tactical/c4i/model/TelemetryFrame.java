package com.tactical.c4i.model;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Binary tactical telemetry frame exchanged between publishers and the receiver.
 * Wire format (little-endian, {@link #FRAME_SIZE} bytes):
 * <pre>
 *   int    nodeId
 *   long   timestamp
 *   double latitude
 *   double longitude
 *   double altitude
 *   float  speed
 *   byte   affiliation (ordinal)
 *   byte   statusFlags
 * </pre>
 */
public final class TelemetryFrame {

    public static final int FRAME_SIZE = 42;

    public enum Affiliation {
        FRIENDLY,
        HOSTILE,
        NEUTRAL,
        UNKNOWN;

        private static final Affiliation[] VALUES = values();

        public static Affiliation fromOrdinal(int ordinal) {
            if (ordinal < 0 || ordinal >= VALUES.length) {
                throw new IllegalArgumentException("Invalid affiliation ordinal: " + ordinal);
            }
            return VALUES[ordinal];
        }
    }

    private final int nodeId;
    private final long timestamp;
    private final double latitude;
    private final double longitude;
    private final double altitude;
    private final float speed;
    private final Affiliation affiliation;
    private final byte statusFlags;

    public TelemetryFrame(
            int nodeId,
            long timestamp,
            double latitude,
            double longitude,
            double altitude,
            float speed,
            Affiliation affiliation,
            byte statusFlags) {
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.speed = speed;
        this.affiliation = Objects.requireNonNull(affiliation, "affiliation");
        this.statusFlags = statusFlags;
    }

    public int nodeId() {
        return nodeId;
    }

    public long timestamp() {
        return timestamp;
    }

    public double latitude() {
        return latitude;
    }

    public double longitude() {
        return longitude;
    }

    public double altitude() {
        return altitude;
    }

    public float speed() {
        return speed;
    }

    public Affiliation affiliation() {
        return affiliation;
    }

    public byte statusFlags() {
        return statusFlags;
    }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(FRAME_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        writeTo(buffer);
        return buffer.array();
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(nodeId);
        buffer.putLong(timestamp);
        buffer.putDouble(latitude);
        buffer.putDouble(longitude);
        buffer.putDouble(altitude);
        buffer.putFloat(speed);
        buffer.put((byte) affiliation.ordinal());
        buffer.put(statusFlags);
    }

    public static TelemetryFrame deserialize(byte[] data) {
        Objects.requireNonNull(data, "data");
        if (data.length < FRAME_SIZE) {
            throw new IllegalArgumentException(
                    "Frame requires " + FRAME_SIZE + " bytes, got " + data.length);
        }
        return deserialize(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN));
    }

    public static TelemetryFrame deserialize(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer");
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.remaining() < FRAME_SIZE) {
            throw new IllegalArgumentException(
                    "Frame requires " + FRAME_SIZE + " bytes, remaining " + buffer.remaining());
        }
        int nodeId = buffer.getInt();
        long timestamp = buffer.getLong();
        double latitude = buffer.getDouble();
        double longitude = buffer.getDouble();
        double altitude = buffer.getDouble();
        float speed = buffer.getFloat();
        Affiliation affiliation = Affiliation.fromOrdinal(buffer.get() & 0xFF);
        byte statusFlags = buffer.get();
        return new TelemetryFrame(
                nodeId, timestamp, latitude, longitude, altitude, speed, affiliation, statusFlags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TelemetryFrame that)) {
            return false;
        }
        return nodeId == that.nodeId
                && timestamp == that.timestamp
                && Double.compare(that.latitude, latitude) == 0
                && Double.compare(that.longitude, longitude) == 0
                && Double.compare(that.altitude, altitude) == 0
                && Float.compare(that.speed, speed) == 0
                && statusFlags == that.statusFlags
                && affiliation == that.affiliation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                nodeId, timestamp, latitude, longitude, altitude, speed, affiliation, statusFlags);
    }

    @Override
    public String toString() {
        return "TelemetryFrame{"
                + "nodeId=" + nodeId
                + ", timestamp=" + timestamp
                + ", latitude=" + latitude
                + ", longitude=" + longitude
                + ", altitude=" + altitude
                + ", speed=" + speed
                + ", affiliation=" + affiliation
                + ", statusFlags=" + statusFlags
                + '}';
    }
}
