package com.tactical.c4i.service;

import com.tactical.c4i.index.SpatialGridIndex;
import com.tactical.c4i.model.TelemetryFrame;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Concurrent telemetry ingestion pipeline backed by virtual threads.
 * Validates frames, updates the spatial index, and raises perimeter alerts for HOSTILE assets.
 */
public final class TelemetryProcessorService implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(TelemetryProcessorService.class.getName());

    public static final double MIN_LATITUDE = -90.0;
    public static final double MAX_LATITUDE = 90.0;
    public static final double MIN_LONGITUDE = -180.0;
    public static final double MAX_LONGITUDE = 180.0;
    public static final double MIN_ALTITUDE = -500.0;
    public static final double MAX_ALTITUDE = 50_000.0;
    public static final float MIN_SPEED = 0.0f;
    public static final float MAX_SPEED = 3_500.0f;

    public record DefensePerimeter(
            String name, double minLat, double maxLat, double minLon, double maxLon) {

        public boolean contains(double latitude, double longitude) {
            return latitude >= minLat
                    && latitude <= maxLat
                    && longitude >= minLon
                    && longitude <= maxLon;
        }
    }

    private final SpatialGridIndex spatialIndex;
    private final List<DefensePerimeter> defensePerimeters;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final LongAdder processedFrames = new LongAdder();
    private final LongAdder rejectedFrames = new LongAdder();
    private final LongAdder perimeterAlerts = new LongAdder();
    private final AtomicLong lastAlertNodeId = new AtomicLong(-1L);
    /** Tracks nodeId|perimeterName pairs already inside a defended zone (alert on entry only). */
    private final Set<String> nodesInsidePerimeters = ConcurrentHashMap.newKeySet();

    public TelemetryProcessorService(SpatialGridIndex spatialIndex) {
        this(
                spatialIndex,
                List.of(
                        new DefensePerimeter("ALPHA-FOB", 34.50, 34.70, -118.40, -118.20),
                        new DefensePerimeter("BRAVO-AIRSTRIP", 34.20, 34.35, -117.90, -117.70),
                        new DefensePerimeter("CHARLIE-COMMS", 33.90, 34.05, -118.10, -117.95)),
                Executors.newVirtualThreadPerTaskExecutor());
    }

    public TelemetryProcessorService(
            SpatialGridIndex spatialIndex,
            List<DefensePerimeter> defensePerimeters,
            ExecutorService executor) {
        this.spatialIndex = Objects.requireNonNull(spatialIndex, "spatialIndex");
        this.defensePerimeters = List.copyOf(Objects.requireNonNull(defensePerimeters, "defensePerimeters"));
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    public void submitRawFrame(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        if (!running.get()) {
            rejectedFrames.increment();
            return;
        }
        byte[] copy = payload.clone();
        try {
            executor.execute(() -> processRawFrame(copy));
        } catch (RejectedExecutionException ex) {
            rejectedFrames.increment();
            LOGGER.log(Level.FINE, "Frame rejected; processor is shutting down", ex);
        }
    }

    public void processFrame(TelemetryFrame frame) {
        Objects.requireNonNull(frame, "frame");
        if (!isValid(frame)) {
            rejectedFrames.increment();
            LOGGER.fine(() -> "Rejected invalid frame for node " + frame.nodeId() + ": " + frame);
            return;
        }

        spatialIndex.updateNode(frame);
        processedFrames.increment();
        evaluatePerimeterAlerts(frame);
    }

    private void processRawFrame(byte[] payload) {
        try {
            TelemetryFrame frame = TelemetryFrame.deserialize(payload);
            processFrame(frame);
        } catch (RuntimeException ex) {
            rejectedFrames.increment();
            LOGGER.log(Level.FINE, "Failed to deserialize telemetry frame", ex);
        }
    }

    private boolean isValid(TelemetryFrame frame) {
        return frame.latitude() >= MIN_LATITUDE
                && frame.latitude() <= MAX_LATITUDE
                && frame.longitude() >= MIN_LONGITUDE
                && frame.longitude() <= MAX_LONGITUDE
                && frame.altitude() >= MIN_ALTITUDE
                && frame.altitude() <= MAX_ALTITUDE
                && frame.speed() >= MIN_SPEED
                && frame.speed() <= MAX_SPEED
                && !Double.isNaN(frame.latitude())
                && !Double.isNaN(frame.longitude())
                && !Double.isNaN(frame.altitude())
                && !Float.isNaN(frame.speed());
    }

    private void evaluatePerimeterAlerts(TelemetryFrame frame) {
        for (DefensePerimeter perimeter : defensePerimeters) {
            String presenceKey = frame.nodeId() + "|" + perimeter.name();
            boolean inside = frame.affiliation() == TelemetryFrame.Affiliation.HOSTILE
                    && perimeter.contains(frame.latitude(), frame.longitude());
            if (inside) {
                if (nodesInsidePerimeters.add(presenceKey)) {
                    perimeterAlerts.increment();
                    lastAlertNodeId.set(frame.nodeId());
                    LOGGER.warning(() -> String.format(
                            "PERIMETER ALERT: HOSTILE node %d entered %s at lat=%.5f lon=%.5f speed=%.1f",
                            frame.nodeId(),
                            perimeter.name(),
                            frame.latitude(),
                            frame.longitude(),
                            frame.speed()));
                }
            } else {
                nodesInsidePerimeters.remove(presenceKey);
            }
        }
    }

    public long getProcessedFrameCount() {
        return processedFrames.sum();
    }

    public long getRejectedFrameCount() {
        return rejectedFrames.sum();
    }

    public long getPerimeterAlertCount() {
        return perimeterAlerts.sum();
    }

    public long getLastAlertNodeId() {
        return lastAlertNodeId.get();
    }

    public int getActiveNodeCount() {
        return spatialIndex.getActiveNodeCount();
    }

    public SpatialGridIndex getSpatialIndex() {
        return spatialIndex;
    }

    public List<DefensePerimeter> getDefensePerimeters() {
        return defensePerimeters;
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
