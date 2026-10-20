package dev.ingest.app.fixture;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deterministic in-process HTTP server serving up to {@code totalRecords} synthetic Dutch RDW
 * vehicle records with Socrata keyset pagination ({@code $limit}, {@code $order}, {@code $where}).
 */
public final class RdwFixtureServer implements AutoCloseable {

    private static final Pattern KEY_PATTERN = Pattern.compile("kenteken\\s*>\\s*'NL(\\d{6})'");
    private static final String[] BRANDS = {"VOLVO", "DAF", "TESLA", "VOLKSWAGEN", "BMW", "TOYOTA", "RENAULT", "AUDI"};
    private static final String[] COLORS = {"ZWART", "WIT", "GRIJS", "BLAUW", "ROOD", "GROEN"};

    private final HttpServer server;
    private final int totalRecords;

    public RdwFixtureServer(int totalRecords) throws IOException {
        this(0, totalRecords);
    }

    public RdwFixtureServer(int port, int totalRecords) throws IOException {
        this.totalRecords = totalRecords;
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        this.server.createContext("/resource/m9d7-ebf2.json", this::handleRequest);
        this.server.start();
    }

    public URI baseUri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
        int limit = Integer.parseInt(params.getOrDefault("$limit", "1000"));
        int startAfter = 0;

        String where = params.get("$where");
        if (where != null) {
            Matcher m = KEY_PATTERN.matcher(where);
            if (m.find()) {
                startAfter = Integer.parseInt(m.group(1));
            } else {
                startAfter = totalRecords;
            }
        }

        int firstIndex = startAfter + 1;
        int lastIndex = Math.min(totalRecords, startAfter + limit);

        StringBuilder sb = new StringBuilder(Math.max(64, (lastIndex - firstIndex + 1) * 180));
        sb.append('[');
        for (int i = firstIndex; i <= lastIndex; i++) {
            if (i > firstIndex) {
                sb.append(',');
            }
            String kenteken = String.format("NL%06d", i);
            String merk = BRANDS[i % BRANDS.length];
            String kleur = COLORS[i % COLORS.length];
            int massa = 1000 + (i % 2500);
            int datum = 20150101 + (i % 90000);
            sb.append("{\"kenteken\":\"").append(kenteken)
              .append("\",\"voertuigsoort\":\"Personenauto\"")
              .append(",\"merk\":\"").append(merk)
              .append("\",\"handelsbenaming\":\"MODEL-").append(i % 50)
              .append("\",\"eerste_kleur\":\"").append(kleur)
              .append("\",\"datum_eerste_toelating\":\"").append(datum)
              .append("\",\"massa_ledig_voertuig\":").append(massa)
              .append('}');
        }
        sb.append(']');

        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, payload.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(payload);
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> map = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return map;
        }
        for (String pair : rawQuery.split("&")) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                String k = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                map.put(k, v);
            }
        }
        return map;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
