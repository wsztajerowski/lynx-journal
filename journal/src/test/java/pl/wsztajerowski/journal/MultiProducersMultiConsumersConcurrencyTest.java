package pl.wsztajerowski.journal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.wsztajerowski.journal.records.JournalByteBuffer;
import pl.wsztajerowski.journal.records.JournalByteBufferFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.Files.createTempFile;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.records.JournalByteBufferFactory.createJournalByteBuffer;

class MultiProducersMultiConsumersConcurrencyTest {
    private static final int PRODUCER_THREADS = 4;
    private static final int CONSUMER_THREADS = 4;
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
    public void testConcurrentWriteAndRead() throws InterruptedException, ExecutionException, TimeoutException {
        BlockingQueue<Location> locationQueue = new LinkedBlockingQueue<>();
        AtomicInteger writesCounter = new AtomicInteger(0);
        AtomicInteger readsCounter = new AtomicInteger(0);
        AtomicLong sum = new AtomicLong(0);
        int iterations = 1_000_000;
        try (ExecutorService executor = newFixedThreadPool(PRODUCER_THREADS + CONSUMER_THREADS)) {
            for (int i = 0; i < PRODUCER_THREADS; i++) {
                executor.submit(createProducer(iterations, locationQueue, writesCounter));
            }
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < CONSUMER_THREADS; i++) {
                futures.add(executor.submit(createConsumer(iterations, locationQueue, readsCounter, sum)));
            }
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS); // Blocks until the task is completed
            }
        }
        long expectedSum = Long.valueOf(iterations)*(iterations-1)/2;
        assertThat(sum)
            .hasValue(expectedSum);
    }

    private Runnable createConsumer(int iterations, BlockingQueue<Location> locationQueue, AtomicInteger readsCounter, AtomicLong sum) {
        return () -> {
            Location location = null;
            try {
                JournalByteBuffer buffer = createJournalByteBuffer(32);
                while (readsCounter.incrementAndGet() < iterations) {
                    buffer.getContentBuffer().clear();
                    location = locationQueue.poll(100, TimeUnit.SECONDS);
                    var variable = sut.readAsync(buffer, location);
                    sum.addAndGet(variable.getInt());
                }
                System.out.println("Last read on thread: " + Thread.currentThread().getName() + " : " + location);
            } catch (Exception e){
                e.printStackTrace();
//                String collected = locationQueue
//                    .stream()
//                    .map(Location::offset)
//                    .map(String::valueOf)
//                    .collect(Collectors.joining(",", "Queue[", "]"));
//                System.out.println(location + ": " + e.getMessage()+ ": " + collected);
                System.exit(1);
            }
        };
    }

    private Runnable createProducer(int iterations, Queue<Location> locationQueue, AtomicInteger writesCounter) {
        return () -> {
            try {
                int iteration;
                while ((iteration = writesCounter.incrementAndGet()) < iterations) {
                    JournalByteBuffer journalByteBuffer = JournalByteBufferFactory.createJournalByteBuffer(128);
                    var buffer = journalByteBuffer.getContentBuffer();
                    buffer.putInt(iteration);
                    buffer.flip();
                    var location = sut.write(journalByteBuffer);
                    locationQueue.offer(location);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        };
    }
}
