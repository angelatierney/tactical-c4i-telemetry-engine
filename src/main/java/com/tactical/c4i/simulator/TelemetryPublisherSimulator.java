package com.tactical.c4i.simulator;

import com.tactical.c4i.model.TelemetryFrame;
import com.tactical.c4i.server.TelemetryReceiverServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Concurrent publisher that simulates 50+ tactical nodes broadcasting telemetry at 10 Hz.
 */
public final class TelemetryPublisherSimulator implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(TelemetryPublisherSimulator.class.getName());

    public static final int DEFAULT_NODE_COUNT = 64;
    public static final int DEFAULT_RATE_HZ = 10;

    private enum NodeKind {
        UAV,
        VEHICLE,
        FIELD_UNIT
    }

    private record SimulatedNode(
            int nodeId,
            NodeKind kind,
            TelemetryFrame.Affiliation affiliation,
            double baseLat,
            double baseLon,
            double altitude,
            float cruiseSpeed,
            double latDrift,
            double lonDrift) {
    }

    private final String host;
    private final int port;
    private final int nodeCount;
    private final int rateHz;
    private final long seed;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final LongAdder framesSent = new LongAdder();
    private final LongAdder sendFailures = new LongAdder();
    private final List<Future<?>> nodeFutures = new ArrayList<>();

    public TelemetryPublisherSimulator() {
        this("127.0.0.1", TelemetryReceiverServer.DEFAULT_PORT, DEFAULT_NODE_COUNT, DEFAULT_RATE_HZ, 42L);
    }

    public TelemetryPublisherSimulator(String host, int port, int nodeCount, int rateHz, long seed) {
        if (nodeCount < 50) {
            throw new IllegalArgumentException("Simulator requires at least 50 nodes");
        }
        if (rateHz <= 0) {
            throw new IllegalArgumentException("rateHz must be positive");
        }
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.nodeCount = nodeCount;
        this.rateHz = rateHz;
        this.seed = seed;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Random random = new Random(seed);
        List<SimulatedNode> nodes = createNodes(random);
        long periodNanos = TimeUnit.SECONDS.toNanos(1) / rateHz;

        for (SimulatedNode node : nodes) {
            nodeFutures.add(executor.submit(() -> runNode(node, periodNanos)));
        }
        LOGGER.info(() -> "TelemetryPublisherSimulator started with " + nodeCount
                + " nodes at " + rateHz + " Hz targeting " + host + ":" + port);
    }

    private List<SimulatedNode> createNodes(Random random) {
        List<SimulatedNode> nodes = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            NodeKind kind = NodeKind.values()[i % NodeKind.values().length];
            TelemetryFrame.Affiliation affiliation = assignAffiliation(i, random);
            double baseLat = 33.8 + random.nextDouble() * 1.0;
            double baseLon = -118.5 + random.nextDouble() * 1.0;
            double altitude = switch (kind) {
                case UAV -> 800.0 + random.nextDouble() * 1_200.0;
                case VEHICLE -> 20.0 + random.nextDouble() * 80.0;
                case FIELD_UNIT -> 5.0 + random.nextDouble() * 40.0;
            };
            float cruiseSpeed = switch (kind) {
                case UAV -> 40.0f + random.nextFloat() * 80.0f;
                case VEHICLE -> 10.0f + random.nextFloat() * 40.0f;
                case FIELD_UNIT -> 1.0f + random.nextFloat() * 8.0f;
            };
            // Seed a few hostiles near defense perimeters for alert testing.
            if (i % 17 == 0) {
                affiliation = TelemetryFrame.Affiliation.HOSTILE;
                baseLat = 34.55 + random.nextDouble() * 0.08;
                baseLon = -118.35 + random.nextDouble() * 0.08;
            } else if (i % 23 == 0) {
                affiliation = TelemetryFrame.Affiliation.HOSTILE;
                baseLat = 34.25 + random.nextDouble() * 0.05;
                baseLon = -117.85 + random.nextDouble() * 0.05;
            }
            nodes.add(new SimulatedNode(
                    1_000 + i,
                    kind,
                    affiliation,
                    baseLat,
                    baseLon,
                    altitude,
                    cruiseSpeed,
                    (random.nextDouble() - 0.5) * 0.0008,
                    (random.nextDouble() - 0.5) * 0.0008));
        }
        return nodes;
    }

    private static TelemetryFrame.Affiliation assignAffiliation(int index, Random random) {
        if (index % 11 == 0) {
            return TelemetryFrame.Affiliation.NEUTRAL;
        }
        if (index % 19 == 0) {
            return TelemetryFrame.Affiliation.UNKNOWN;
        }
        if (random.nextDouble() < 0.08) {
            return TelemetryFrame.Affiliation.HOSTILE;
        }
        return TelemetryFrame.Affiliation.FRIENDLY;
    }

    private void runNode(SimulatedNode node, long periodNanos) {
        Random random = new Random(seed ^ node.nodeId());
        int attempt = 0;
        while (running.get() && attempt < 40) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 2_000);
                socket.setTcpNoDelay(true);
                try (OutputStream output = socket.getOutputStream()) {
                    long nextDeadline = System.nanoTime();
                    int tick = 0;
                    while (running.get()) {
                        TelemetryFrame frame = nextFrame(node, random, tick++);
                        output.write(frame.serialize());
                        output.flush();
                        framesSent.increment();

                        nextDeadline += periodNanos;
                        long sleepNanos = nextDeadline - System.nanoTime();
                        if (sleepNanos > 0) {
                            TimeUnit.NANOSECONDS.sleep(sleepNanos);
                        } else {
                            nextDeadline = System.nanoTime();
                        }
                    }
                }
                return;
            } catch (IOException | InterruptedException ex) {
                sendFailures.increment();
                if (!running.get()) {
                    if (ex instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    return;
                }
                attempt++;
                try {
                    TimeUnit.MILLISECONDS.sleep(100L * attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (attempt >= 40) {
                    LOGGER.log(Level.WARNING, "Node " + node.nodeId() + " failed to stay connected", ex);
                }
            }
        }
    }

    private TelemetryFrame nextFrame(SimulatedNode node, Random random, int tick) {
        double latitude = node.baseLat() + (node.latDrift() * tick) + ((random.nextDouble() - 0.5) * 0.002);
        double longitude = node.baseLon() + (node.lonDrift() * tick) + ((random.nextDouble() - 0.5) * 0.002);
        double altitude = node.altitude() + ((random.nextDouble() - 0.5) * 15.0);
        float speed = Math.max(0.0f, node.cruiseSpeed() + ((random.nextFloat() - 0.5f) * 5.0f));
        TelemetryFrame.Affiliation affiliation = node.affiliation();
        byte statusFlags = (byte) (tick & 0x0F);

        // Occasional anomalies to exercise validation (staggered per node).
        int anomalyPhase = Math.floorMod(node.nodeId(), 17);
        if (tick > 0 && (tick + anomalyPhase) % 97 == 0) {
            latitude = 120.0 + random.nextDouble() * 20.0;
        } else if (tick > 0 && (tick + anomalyPhase) % 113 == 0) {
            longitude = -220.0 - random.nextDouble() * 10.0;
        } else if (tick > 0 && (tick + anomalyPhase) % 131 == 0) {
            speed = 9_999.0f;
        }

        // Occasional hostile pop-up inside a perimeter (subset of nodes, staggered).
        if (tick > 0 && node.nodeId() % 7 == 0 && (tick + anomalyPhase) % 150 == 0) {
            affiliation = TelemetryFrame.Affiliation.HOSTILE;
            latitude = 33.95 + random.nextDouble() * 0.05;
            longitude = -118.05 + random.nextDouble() * 0.05;
            speed = 55.0f + random.nextFloat() * 20.0f;
            statusFlags = (byte) 0xA5;
        }

        return new TelemetryFrame(
                node.nodeId(),
                System.currentTimeMillis(),
                latitude,
                longitude,
                altitude,
                speed,
                affiliation,
                statusFlags);
    }

    public long getFramesSent() {
        return framesSent.sum();
    }

    public long getSendFailures() {
        return sendFailures.sum();
    }

    public int getNodeCount() {
        return nodeCount;
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            executor.shutdownNow();
            return;
        }
        for (Future<?> future : nodeFutures) {
            future.cancel(true);
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warning("Simulator executor did not terminate cleanly");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info(() -> "TelemetryPublisherSimulator stopped after sending " + framesSent.sum() + " frames");
    }
}
