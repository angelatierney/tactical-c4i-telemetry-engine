package com.tactical.c4i.server;

import com.tactical.c4i.model.TelemetryFrame;
import com.tactical.c4i.service.TelemetryProcessorService;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Multi-threaded TCP telemetry receiver listening on a fixed port.
 * Each accepted connection is handled on a virtual thread for high concurrency.
 */
public final class TelemetryReceiverServer implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(TelemetryReceiverServer.class.getName());

    public static final int DEFAULT_PORT = 9090;

    private final int port;
    private final TelemetryProcessorService processorService;
    private final ExecutorService acceptExecutor;
    private final ExecutorService connectionExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final LongAdder framesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final LongAdder connectionErrors = new LongAdder();

    private volatile ServerSocket serverSocket;
    private volatile long startedAtNanos;

    public TelemetryReceiverServer(TelemetryProcessorService processorService) {
        this(DEFAULT_PORT, processorService);
    }

    public TelemetryReceiverServer(int port, TelemetryProcessorService processorService) {
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        this.port = port;
        this.processorService = Objects.requireNonNull(processorService, "processorService");
        this.acceptExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "telemetry-accept");
            thread.setDaemon(true);
            return thread;
        });
        this.connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public synchronized void start() throws IOException {
        if (running.get()) {
            return;
        }
        ServerSocket socket = new ServerSocket();
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress(port));
        socket.setSoTimeout(500);
        this.serverSocket = socket;
        this.startedAtNanos = System.nanoTime();
        running.set(true);
        acceptExecutor.execute(this::acceptLoop);
        LOGGER.info(() -> "TelemetryReceiverServer listening on port " + port);
    }

    private void acceptLoop() {
        while (running.get()) {
            ServerSocket localSocket = serverSocket;
            if (localSocket == null || localSocket.isClosed()) {
                break;
            }
            try {
                Socket client = localSocket.accept();
                client.setTcpNoDelay(true);
                activeConnections.incrementAndGet();
                connectionExecutor.execute(() -> handleClient(client));
            } catch (SocketTimeoutException ignored) {
                // Periodic wake-up to observe shutdown.
            } catch (IOException ex) {
                if (running.get()) {
                    connectionErrors.increment();
                    LOGGER.log(Level.WARNING, "Accept loop failure", ex);
                }
            }
        }
    }

    private void handleClient(Socket client) {
        String remote = String.valueOf(client.getRemoteSocketAddress());
        try (client; InputStream input = client.getInputStream()) {
            byte[] frameBuffer = new byte[TelemetryFrame.FRAME_SIZE];
            while (running.get()) {
                int offset = 0;
                while (offset < TelemetryFrame.FRAME_SIZE) {
                    int read = input.read(frameBuffer, offset, TelemetryFrame.FRAME_SIZE - offset);
                    if (read < 0) {
                        return;
                    }
                    offset += read;
                }
                framesReceived.increment();
                bytesReceived.add(TelemetryFrame.FRAME_SIZE);
                processorService.submitRawFrame(frameBuffer);
            }
        } catch (IOException ex) {
            if (running.get()) {
                connectionErrors.increment();
                LOGGER.log(Level.FINE, "Client connection closed: " + remote, ex);
            }
        } finally {
            activeConnections.decrementAndGet();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public int getPort() {
        return port;
    }

    public int getActiveConnections() {
        return activeConnections.get();
    }

    public long getFramesReceived() {
        return framesReceived.sum();
    }

    public long getBytesReceived() {
        return bytesReceived.sum();
    }

    public long getConnectionErrors() {
        return connectionErrors.sum();
    }

    public double getFramesPerSecond() {
        long elapsedNanos = System.nanoTime() - startedAtNanos;
        if (elapsedNanos <= 0L || !running.get() && framesReceived.sum() == 0L) {
            return 0.0;
        }
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        if (elapsedSeconds <= 0.0) {
            return 0.0;
        }
        return framesReceived.sum() / elapsedSeconds;
    }

    @Override
    public synchronized void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        ServerSocket localSocket = serverSocket;
        if (localSocket != null) {
            try {
                localSocket.close();
            } catch (IOException ex) {
                LOGGER.log(Level.FINE, "Error closing server socket", ex);
            }
        }
        acceptExecutor.shutdownNow();
        connectionExecutor.shutdown();
        try {
            if (!connectionExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                connectionExecutor.shutdownNow();
            }
            if (!acceptExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                acceptExecutor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            connectionExecutor.shutdownNow();
            acceptExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOGGER.info("TelemetryReceiverServer shut down");
    }
}
