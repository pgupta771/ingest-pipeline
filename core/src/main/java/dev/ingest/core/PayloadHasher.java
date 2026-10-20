package dev.ingest.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

/**
 * Computes deterministic SHA-256 hashes of record payloads independent of map key ordering.
 */
public final class PayloadHasher {

    private static final ObjectMapper CANONICAL_MAPPER = new ObjectMapper()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    private PayloadHasher() {
    }

    /**
     * Returns a lowercase hex SHA-256 digest of the canonicalized payload map.
     */
    public static String sha256(Map<String, Object> payload) {
        try {
            Map<String, Object> sorted = new TreeMap<>(payload);
            byte[] canonicalBytes = CANONICAL_MAPPER.writeValueAsString(sorted)
                    .getBytes(StandardCharsets.UTF_8);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(canonicalBytes));
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to hash record payload", e);
        }
    }
}
