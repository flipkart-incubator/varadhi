package com.flipkart.varadhi.web.entities;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Data
@Slf4j
public class DlqPageMarker {
    private static final Pattern validPageMarkerPattern = Pattern.compile("\\d+=([^~]+)(?:[~]\\d+=([^~]+))*[~]?");
    /*
     * Subscription marker format is
     * shardId=shardMarker[~shardId=shardMarker]{0..n}
     * Assumption: shardMarker doesn't use ~ and = characters.
     *
     * shardMarker format is opaque string interpreted by consumers.
     * e.g. for Pulsar it could be mId:legerId:entryId:partitionIndex
     */
    private final Map<Integer, String> shardPageMarkers;

    public static DlqPageMarker fromString(String str) {
        if (str.isBlank()) {
            return new DlqPageMarker(Map.of());
        }
        if (!validPageMarkerPattern.matcher(str).matches()) {
            throw new IllegalArgumentException("Invalid page marker: " + str);
        }
        String[] markers = str.split("[~]");
        return new DlqPageMarker(Arrays.stream(markers).filter(s -> !s.isBlank()).map(marker -> {
            String[] parts = marker.split("=");
            return Map.entry(Integer.parseInt(parts[0]), parts[1]);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        List<String> markers =
                shardPageMarkers.entrySet().stream().map(e -> String.format("%d=%s", e.getKey(), e.getValue()))
                        .toList();
        return Strings.join(markers, '~');
    }

    public void addShardMarker(int shardId, String marker) {
        if (shardPageMarkers.containsKey(shardId)) {
            throw new IllegalArgumentException("Marker for shard " + shardId + " already exists");
        }
        if (marker != null && !marker.isBlank()) {
            shardPageMarkers.put(shardId, marker);
        }
    }

    public boolean hasMarkers() {
        return !shardPageMarkers.isEmpty();
    }

    public boolean hasMarker(int shardId) {
        return shardPageMarkers.containsKey(shardId);
    }

    public String getShardMarker(int shardId) {
        return shardPageMarkers.get(shardId);
    }
}
