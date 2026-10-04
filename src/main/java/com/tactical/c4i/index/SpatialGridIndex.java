package com.tactical.c4i.index;

import com.tactical.c4i.model.TelemetryFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory spatial index that buckets nodes into a lat/lon grid.
 * Updates and bounding-box queries are safe for concurrent writers and readers.
 */
public final class SpatialGridIndex {

    private final double cellSizeDegrees;
    private final ConcurrentHashMap<Integer, TelemetryFrame> nodesById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Set<Integer>> cellToNodeIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Long> nodeToCell = new ConcurrentHashMap<>();

    public SpatialGridIndex() {
        this(0.05);
    }

    public SpatialGridIndex(double cellSizeDegrees) {
        if (cellSizeDegrees <= 0.0) {
            throw new IllegalArgumentException("cellSizeDegrees must be positive");
        }
        this.cellSizeDegrees = cellSizeDegrees;
    }

    public void updateNode(TelemetryFrame frame) {
        Objects.requireNonNull(frame, "frame");
        int nodeId = frame.nodeId();
        long newCell = cellKey(frame.latitude(), frame.longitude());

        Long previousCell = nodeToCell.put(nodeId, newCell);
        nodesById.put(nodeId, frame);

        if (previousCell != null && previousCell != newCell) {
            removeFromCell(previousCell, nodeId);
        }
        cellToNodeIds
                .computeIfAbsent(newCell, ignored -> ConcurrentHashMap.newKeySet())
                .add(nodeId);
    }

    public List<TelemetryFrame> getNodesInBoundingBox(
            double minLat, double maxLat, double minLon, double maxLon) {
        if (minLat > maxLat || minLon > maxLon) {
            throw new IllegalArgumentException("Invalid bounding box extents");
        }

        int minCellLat = toCellIndex(minLat);
        int maxCellLat = toCellIndex(maxLat);
        int minCellLon = toCellIndex(minLon);
        int maxCellLon = toCellIndex(maxLon);

        List<TelemetryFrame> results = new ArrayList<>();
        for (int cellLat = minCellLat; cellLat <= maxCellLat; cellLat++) {
            for (int cellLon = minCellLon; cellLon <= maxCellLon; cellLon++) {
                Set<Integer> nodeIds = cellToNodeIds.get(packCell(cellLat, cellLon));
                if (nodeIds == null || nodeIds.isEmpty()) {
                    continue;
                }
                for (Integer nodeId : nodeIds) {
                    TelemetryFrame frame = nodesById.get(nodeId);
                    if (frame != null
                            && frame.latitude() >= minLat
                            && frame.latitude() <= maxLat
                            && frame.longitude() >= minLon
                            && frame.longitude() <= maxLon) {
                        results.add(frame);
                    }
                }
            }
        }
        return results;
    }

    public int getActiveNodeCount() {
        return nodesById.size();
    }

    public TelemetryFrame getNode(int nodeId) {
        return nodesById.get(nodeId);
    }

    public Map<Integer, TelemetryFrame> snapshot() {
        return Map.copyOf(nodesById);
    }

    private void removeFromCell(long cell, int nodeId) {
        Set<Integer> nodeIds = cellToNodeIds.get(cell);
        if (nodeIds == null) {
            return;
        }
        nodeIds.remove(nodeId);
        if (nodeIds.isEmpty()) {
            cellToNodeIds.remove(cell, nodeIds);
        }
    }

    private long cellKey(double latitude, double longitude) {
        return packCell(toCellIndex(latitude), toCellIndex(longitude));
    }

    private int toCellIndex(double coordinate) {
        return (int) Math.floor(coordinate / cellSizeDegrees);
    }

    private static long packCell(int cellLat, int cellLon) {
        return (((long) cellLat) << 32) ^ (cellLon & 0xFFFFFFFFL);
    }
}
