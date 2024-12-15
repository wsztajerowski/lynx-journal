package pl.wsztajerowski.journal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.Files.createTempFile;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.FilesTestUtils.readAsUtf8;

class SingleProducerSingleConsumerConcurrencyTest {
    private Journal sut;

    @BeforeEach
    void setUp() throws IOException {
        Path dataFilePath = createTempFile("journal", ".dat");
        sut = Journal.open(dataFilePath, false);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.close();
    }

    @Test
    public void testConcurrentWriteAndRead() throws InterruptedException {
        Queue<Location> locationQueue = new ConcurrentLinkedQueue<>();
        AtomicInteger counter = new AtomicInteger(0);
        AtomicBoolean producerFails = new AtomicBoolean(false);
        int iterations = 1_000;
        try (ExecutorService executor = newFixedThreadPool(2)) {
            executor.submit(createProducer(iterations, locationQueue, producerFails));
            executor.submit(createConsumer(iterations, locationQueue, producerFails, counter));
            executor.shutdown();
            assertThat(executor.awaitTermination(1, TimeUnit.SECONDS))
                .isTrue();
        }
        assertThat(counter)
            .hasValue(iterations);
    }

    private Runnable createConsumer(int iterations, Queue<Location> locationQueue, AtomicBoolean producerFails, AtomicInteger counter) {
        return () -> {
            ByteBuffer buffer = ByteBuffer.allocate(32);
            for (int i = 0; i < iterations; i++) {
                buffer.clear();
                Location location;
                while ((location = locationQueue.poll()) == null) {
                    if (producerFails.get()) {
                        throw new RuntimeException("Producer fails");
                    }
                }
                var variable = sut.read(buffer, location);
                counter.incrementAndGet();
                String actual = readAsUtf8(variable);
                assertThat(actual)
                    .isEqualTo("TEST DATA - %08d".formatted(i));
            }
        };
    }

    private Runnable createProducer(int iterations, Queue<Location> locationQueue, AtomicBoolean producerFails) {
        return () -> {
            try {
                for (int i = 0; i < iterations; i++) {
                    JournalByteBuffer journalByteBuffer = FilesTestUtils.wrapInJournalByteBuffer("TEST DATA - %08d".formatted(i));
                    var location = sut.write(journalByteBuffer);
                    locationQueue.add(location);
                }
            } catch (Exception e) {
                producerFails.set(true);
                throw new RuntimeException(e);
            }
        };
    }
}
