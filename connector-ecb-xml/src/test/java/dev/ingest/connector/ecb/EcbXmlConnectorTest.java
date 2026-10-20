package dev.ingest.connector.ecb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.ingest.core.ConnectorException;
import dev.ingest.core.Cursor;
import dev.ingest.core.Page;
import dev.ingest.core.SourceSession;
import java.io.BufferedWriter;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EcbXmlConnectorTest {

    @TempDir
    Path tempDir;

    @Test
    void streamsAndResumesFromMidFileCursor() throws Exception {
        Path xml = tempDir.resolve("eurofxref-hist.xml");
        Files.writeString(xml, """
                <?xml version="1.0" encoding="UTF-8"?>
                <gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
                                 xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
                  <Cube>
                    <Cube time="2026-10-19">
                      <Cube currency="USD" rate="1.0850"/>
                      <Cube currency="GBP" rate="0.8420"/>
                    </Cube>
                    <Cube time="2026-10-20">
                      <Cube currency="USD" rate="1.0910"/>
                      <Cube currency="JPY" rate="162.45"/>
                    </Cube>
                  </Cube>
                </gesmes:Envelope>
                """);

        EcbXmlConnector connector = new EcbXmlConnector(xml);

        // Read first 2 records
        Optional<Cursor> savedCursor;
        try (SourceSession s1 = connector.openSession(EcbXmlConnector.STREAM_NAME, Optional.empty())) {
            Page p1 = s1.nextPage(2);
            assertThat(p1.records()).extracting(r -> r.primaryKey())
                    .containsExactly("2026-10-19_USD", "2026-10-19_GBP");
            assertThat(p1.nextCursor()).contains(Cursor.of("eurofxref-hist.xml:2"));
            assertThat(p1.hasMore()).isTrue();
            savedCursor = p1.nextCursor();
        }

        // Resume from savedCursor ("eurofxref-hist.xml:2") and read remaining 2 records
        try (SourceSession s2 = connector.openSession(EcbXmlConnector.STREAM_NAME, savedCursor)) {
            Page p2 = s2.nextPage(2);
            assertThat(p2.records()).extracting(r -> r.primaryKey())
                    .containsExactly("2026-10-20_USD", "2026-10-20_JPY");
            assertThat(p2.nextCursor()).contains(Cursor.of("eurofxref-hist.xml:4"));
            assertThat(p2.hasMore()).isFalse();
        }
    }

    @Test
    void rejectsMalformedXml() throws Exception {
        Path badXml = tempDir.resolve("corrupt.xml");
        Files.writeString(badXml, "<gesmes:Envelope><Cube><Cube time='2026-10-20'><unclosed>");

        EcbXmlConnector connector = new EcbXmlConnector(badXml);
        try (SourceSession session = connector.openSession(EcbXmlConnector.STREAM_NAME, Optional.empty())) {
            assertThatThrownBy(() -> session.nextPage(10))
                    .isInstanceOf(ConnectorException.class)
                    .satisfies(e -> assertThat(((ConnectorException) e).reasonCode()).isEqualTo("MALFORMED_XML"));
        }
    }

    @Test
    void streams150MbSyntheticXmlFileUnder64MbHeapWithoutOom() throws Exception {
        Path largeXml = tempDir.resolve("synthetic-155mb.xml");
        generateSyntheticXml(largeXml, 155 * 1024 * 1024L);
        long sizeMb = Files.size(largeXml) / (1024 * 1024);
        assertThat(sizeMb).isGreaterThanOrEqualTo(150L);

        String classpath = System.getProperty("java.class.path");
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();

        Process process = new ProcessBuilder(
                javaBin,
                "-Xms32m",
                "-Xmx64m",
                "-cp",
                classpath,
                HeapBoundedRunner.class.getName(),
                largeXml.toString())
                .redirectErrorStream(true)
                .start();

        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();

        assertThat(exitCode)
                .as("Child JVM (-Xmx64m) output: %s", output)
                .isZero();
        assertThat(output).contains("HEAP_TEST_SUCCESS");
    }

    private static void generateSyntheticXml(Path target, long minBytes) throws Exception {
        String[] currencies = {"USD", "GBP", "JPY", "CHF", "CAD", "AUD", "SEK", "NOK", "DKK", "PLN"};
        try (BufferedWriter writer = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            writer.write("<gesmes:Envelope xmlns:gesmes=\"http://www.gesmes.org/xml/2002-08-01\">\n<Cube>\n");
            long written = 120L;
            int dayCounter = 0;
            while (written < minBytes) {
                dayCounter++;
                String dateStr = String.format("2020-%02d-%02d", (dayCounter % 12) + 1, (dayCounter % 28) + 1);
                String openDay = "  <Cube time=\"" + dateStr + "\">\n";
                writer.write(openDay);
                written += openDay.length();
                for (int i = 0; i < currencies.length; i++) {
                    String line = "    <Cube currency=\"" + currencies[i] + "\" rate=\"1." + (1000 + (dayCounter + i) % 8000) + "\"/>\n";
                    writer.write(line);
                    written += line.length();
                }
                String closeDay = "  </Cube>\n";
                writer.write(closeDay);
                written += closeDay.length();
            }
            writer.write("</Cube>\n</gesmes:Envelope>\n");
        }
    }

    /**
     * Entry point executed inside a forked {@code -Xmx64m} JVM to prove O(1) memory streaming on 150MB+ XML.
     */
    public static final class HeapBoundedRunner {
        public static void main(String[] args) {
            Path xmlPath = Path.of(args[0]);
            EcbXmlConnector connector = new EcbXmlConnector(xmlPath);
            long totalRecords = 0L;
            long peakHeapBytes = 0L;

            try (SourceSession session = connector.openSession(EcbXmlConnector.STREAM_NAME, Optional.empty())) {
                while (true) {
                    Page page = session.nextPage(4000);
                    totalRecords += page.records().size();
                    long used = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
                    if (used > peakHeapBytes) {
                        peakHeapBytes = used;
                    }
                    if (!page.hasMore() || page.records().isEmpty()) {
                        break;
                    }
                }
            }
            long peakMb = peakHeapBytes / (1024 * 1024);
            System.out.println("HEAP_TEST_SUCCESS records=" + totalRecords + " peakHeapMB=" + peakMb);
        }
    }
}
