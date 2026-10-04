package com.tactical.c4i;

import com.tactical.c4i.index.SpatialGridIndex;
import com.tactical.c4i.server.TelemetryReceiverServer;
import com.tactical.c4i.service.TelemetryProcessorService;
import com.tactical.c4i.simulator.TelemetryPublisherSimulator;

import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Boots the telemetry receiver, launches the multi-node simulator, prints situational
 * awareness metrics for 15 seconds, then shuts the system down gracefully.
 */
public final class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final int RUN_SECONDS = 15;
    private static final int METRIC_INTERVAL_SECONDS = 1;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        configureLogging();

        SpatialGridIndex spatialIndex = new SpatialGridIndex();
        try (TelemetryProcessorService processor = new TelemetryProcessorService(spatialIndex);
             TelemetryReceiverServer server = new TelemetryReceiverServer(processor);
             TelemetryPublisherSimulator simulator = new TelemetryPublisherSimulator()) {

            server.start();
            // Allow the accept loop to bind before publishers connect.
            TimeUnit.MILLISECONDS.sleep(250);
            simulator.start();

            LOGGER.info("System live — collecting situational awareness for " + RUN_SECONDS + " seconds");
            long previousProcessed = 0L;
            long previousReceived = 0L;

            for (int second = 1; second <= RUN_SECONDS; second++) {
                TimeUnit.SECONDS.sleep(METRIC_INTERVAL_SECONDS);

                long processed = processor.getProcessedFrameCount();
                long received = server.getFramesReceived();
                long processedDelta = processed - previousProcessed;
                long receivedDelta = received - previousReceived;
                previousProcessed = processed;
                previousReceived = received;

                System.out.printf(
                        "[t+%02ds] activeNodes=%d processedTotal=%d frames/sec=%d received/sec=%d "
                                + "rejected=%d perimeterAlerts=%d connections=%d framesSent=%d%n",
                        second,
                        processor.getActiveNodeCount(),
                        processed,
                        processedDelta,
                        receivedDelta,
                        processor.getRejectedFrameCount(),
                        processor.getPerimeterAlertCount(),
                        server.getActiveConnections(),
                        simulator.getFramesSent());
            }

            LOGGER.info("Run complete — initiating graceful shutdown");
        }

        LOGGER.info("tactical-c4i-telemetry-engine shutdown complete");
    }

    private static void configureLogging() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        if (root.getHandlers().length == 0) {
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(Level.INFO);
            handler.setFormatter(new SimpleFormatter());
            root.addHandler(handler);
        } else {
            for (var handler : root.getHandlers()) {
                handler.setLevel(Level.INFO);
            }
        }
        Logger.getLogger("com.tactical.c4i").setLevel(Level.INFO);
    }
}
