# tactical-c4i-telemetry-engine

Java 21 tactical C4I telemetry engine that ingests binary node position reports over TCP, indexes them in a concurrent spatial grid, and raises perimeter alerts when HOSTILE assets enter defended zones.

## Architecture

| Component | Role |
|-----------|------|
| `TelemetryFrame` | Fixed-size little-endian binary frame (42 bytes) with affiliation and kinematics |
| `SpatialGridIndex` | Thread-safe lat/lon cell index backed by `ConcurrentHashMap` |
| `TelemetryProcessorService` | Virtual-thread pipeline: validate → index → perimeter alert |
| `TelemetryReceiverServer` | Multi-connection TCP server on port **9090** |
| `TelemetryPublisherSimulator` | 64 concurrent nodes (UAVs, vehicles, field units) publishing at **10 Hz** |
| `Main` | Starts server + simulator, prints metrics for 15 seconds, then shuts down |

## Requirements

- JDK 21+
- Apache Maven 3.9+

## Build

```bash
mvn -q clean package
```

## Run

```bash
mvn -q exec:java
```

Or after packaging:

```bash
java -jar target/tactical-c4i-telemetry-engine-1.0.0.jar
```

## What you will see

While running, `Main` prints one situational awareness line per second, for example:

```text
[t+01s] activeNodes=64 processedTotal=512 frames/sec=512 received/sec=520 rejected=0 perimeterAlerts=2 connections=64 framesSent=540
```

HOSTILE entries into the ALPHA-FOB, BRAVO-AIRSTRIP, or CHARLIE-COMMS perimeters emit `PERIMETER ALERT` warnings. Occasional out-of-range lat/lon/speed values from the simulator are rejected by validation and counted under `rejected`.

## Binary frame layout

Little-endian, 42 bytes:

| Offset | Type | Field |
|--------|------|-------|
| 0 | `int` | `nodeId` |
| 4 | `long` | `timestamp` |
| 12 | `double` | `latitude` |
| 20 | `double` | `longitude` |
| 28 | `double` | `altitude` |
| 36 | `float` | `speed` |
| 40 | `byte` | `affiliation` ordinal (`FRIENDLY`, `HOSTILE`, `NEUTRAL`, `UNKNOWN`) |
| 41 | `byte` | `statusFlags` |

## Project layout

```text
src/main/java/com/tactical/c4i/
  Main.java
  model/TelemetryFrame.java
  index/SpatialGridIndex.java
  service/TelemetryProcessorService.java
  server/TelemetryReceiverServer.java
  simulator/TelemetryPublisherSimulator.java
pom.xml
README.md
```
