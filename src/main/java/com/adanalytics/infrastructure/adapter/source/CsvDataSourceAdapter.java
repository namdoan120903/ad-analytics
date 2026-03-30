package com.adanalytics.infrastructure.adapter.source;

import com.adanalytics.application.port.DataSourcePort;
import com.adanalytics.domain.model.AdRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * High-performance CSV data source adapter.
 * <p>
 * Supports two modes:
 * <ul>
 * <li><b>BufferedReader mode</b>: standard line-by-line streaming</li>
 * <li><b>Memory-mapped mode</b>: FileChannel + MappedByteBuffer for maximum
 * throughput</li>
 * </ul>
 * <p>
 * Uses a zero-copy CSV parser that avoids String.split() — scans for commas
 * using
 * index tracking with substring() for minimal allocation.
 */
public class CsvDataSourceAdapter implements DataSourcePort {

    private static final Logger log = LoggerFactory.getLogger(CsvDataSourceAdapter.class);
    private static final int MMAP_CHUNK_SIZE = 256 * 1024 * 1024; // 256MB chunks for memory mapping

    private final Path filePath;
    private final boolean useMemoryMapped;
    private final LongAdder skippedRows = new LongAdder();

    public CsvDataSourceAdapter(Path filePath, boolean useMemoryMapped) {
        this.filePath = filePath;
        this.useMemoryMapped = useMemoryMapped;
    }

    @Override
    public Stream<AdRecord> streamRecords() {
        if (useMemoryMapped) {
            return streamMemoryMapped();
        }
        return streamBuffered();
    }

    @Override
    public String getSourceName() {
        return "CSV [" + filePath.getFileName() + "]"
                + (useMemoryMapped ? " (memory-mapped)" : " (buffered)");
    }

    public long getSkippedRowCount() {
        return skippedRows.sum();
    }

    // ── BufferedReader mode ──────────────────────────────────────────────

    private Stream<AdRecord> streamBuffered() {
        try {
            BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8);
            // Skip header line if present
            String firstLine = reader.readLine();
            boolean hasHeader = firstLine != null && firstLine.startsWith("campaign_id");

            Stream<String> lineStream = reader.lines();
            if (!hasHeader && firstLine != null) {
                // First line is data, prepend it
                lineStream = Stream.concat(Stream.of(firstLine), lineStream);
            }

            return lineStream
                    .map(this::parseLine)
                    .filter(java.util.Objects::nonNull)
                    .onClose(() -> {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            log.warn("Error closing reader", e);
                        }
                        if (skippedRows.sum() > 0) {
                            log.warn("Skipped {} malformed rows", skippedRows.sum());
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open CSV file: " + filePath, e);
        }
    }

    // ── Memory-Mapped mode ───────────────────────────────────────────────

    private Stream<AdRecord> streamMemoryMapped() {
        try {
            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
            long fileSize = channel.size();
            log.info("Memory-mapping file: {} ({} MB)", filePath, fileSize / (1024 * 1024));

            Iterator<AdRecord> iterator = new MemoryMappedIterator(channel, fileSize);
            Spliterator<AdRecord> spliterator = Spliterators.spliteratorUnknownSize(
                    iterator, Spliterator.ORDERED | Spliterator.NONNULL);

            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> {
                        try {
                            channel.close();
                        } catch (IOException e) {
                            log.warn("Error closing channel", e);
                        }
                        if (skippedRows.sum() > 0) {
                            log.warn("Skipped {} malformed rows", skippedRows.sum());
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to memory-map CSV file: " + filePath, e);
        }
    }

    /**
     * Iterator that reads through memory-mapped file chunks.
     */
    private class MemoryMappedIterator implements Iterator<AdRecord> {
        private final FileChannel channel;
        private final long fileSize;
        private long position;
        private ByteBuffer currentBuffer;
        private long currentChunkStart;
        private long currentChunkEnd;
        private boolean headerSkipped;
        private AdRecord nextRecord;
        private boolean hasNext;
        private final StringBuilder lineBuilder = new StringBuilder(256);

        MemoryMappedIterator(FileChannel channel, long fileSize) throws IOException {
            this.channel = channel;
            this.fileSize = fileSize;
            this.position = 0;
            mapNextChunk();
            // Skip header
            skipHeader();
            advance();
        }

        private void mapNextChunk() throws IOException {
            if (position >= fileSize) {
                currentBuffer = null;
                return;
            }
            long remaining = fileSize - position;
            long chunkSize = Math.min(remaining, MMAP_CHUNK_SIZE);
            currentBuffer = channel.map(FileChannel.MapMode.READ_ONLY, position, chunkSize);
            currentChunkStart = position;
            currentChunkEnd = position + chunkSize;
        }

        private void skipHeader() throws IOException {
            if (currentBuffer == null)
                return;
            // Read first line to check if header
            String firstLine = readLine();
            if (firstLine != null && !firstLine.startsWith("campaign_id")) {
                // First line is data, parse it
                AdRecord record = parseLine(firstLine);
                if (record != null) {
                    nextRecord = record;
                    hasNext = true;
                }
            }
            headerSkipped = true;
        }

        private String readLine() throws IOException {
            lineBuilder.setLength(0);
            while (true) {
                if (currentBuffer == null)
                    return lineBuilder.length() > 0 ? lineBuilder.toString() : null;
                if (!currentBuffer.hasRemaining()) {
                    position = currentChunkEnd;
                    mapNextChunk();
                    if (currentBuffer == null) {
                        return lineBuilder.length() > 0 ? lineBuilder.toString() : null;
                    }
                }
                byte b = currentBuffer.get();
                if (b == '\n') {
                    return lineBuilder.toString();
                } else if (b != '\r') {
                    lineBuilder.append((char) b);
                }
            }
        }

        private void advance() {
            if (nextRecord != null)
                return; // already have a buffered record from header skip
            try {
                while (true) {
                    String line = readLine();
                    if (line == null) {
                        hasNext = false;
                        return;
                    }
                    if (line.isEmpty())
                        continue;
                    AdRecord record = parseLine(line);
                    if (record != null) {
                        nextRecord = record;
                        hasNext = true;
                        return;
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public AdRecord next() {
            if (!hasNext)
                throw new NoSuchElementException();
            AdRecord result = nextRecord;
            nextRecord = null;
            advance();
            return result;
        }
    }

    // ── Zero-copy CSV parser ─────────────────────────────────────────────

    /**
     * Parse a CSV line without using String.split().
     * Uses index-based scanning for comma positions, then substring().
     * Returns null for malformed rows (graceful skip).
     */
    AdRecord parseLine(String line) {
        try {
            if (line == null || line.isEmpty())
                return null;

            int len = line.length();
            int fieldStart = 0;
            int fieldIndex = 0;

            String campaignId = null;
            String date = null;
            long impressions = 0;
            long clicks = 0;
            double spend = 0.0;
            long conversions = 0;

            for (int i = 0; i <= len; i++) {
                if (i == len || line.charAt(i) == ',') {
                    String value = line.substring(fieldStart, i).trim();
                    switch (fieldIndex) {
                        case 0 -> campaignId = value;
                        case 1 -> date = value;
                        case 2 -> impressions = Long.parseLong(value);
                        case 3 -> clicks = Long.parseLong(value);
                        case 4 -> spend = Double.parseDouble(value);
                        case 5 -> conversions = Long.parseLong(value);
                    }
                    fieldIndex++;
                    fieldStart = i + 1;
                }
            }

            if (fieldIndex < 6) {
                skippedRows.increment();
                log.debug("Skipping malformed row (only {} fields): {}", fieldIndex, line);
                return null;
            }

            return new AdRecord(campaignId, date, impressions, clicks, spend, conversions);
        } catch (NumberFormatException e) {
            skippedRows.increment();
            log.debug("Skipping row with invalid number: {}", line);
            return null;
        } catch (IllegalArgumentException e) {
            skippedRows.increment();
            log.debug("Skipping invalid row: {} — {}", line, e.getMessage());
            return null;
        }
    }
}
