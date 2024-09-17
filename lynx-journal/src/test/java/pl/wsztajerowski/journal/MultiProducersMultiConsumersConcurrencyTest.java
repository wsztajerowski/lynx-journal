package pl.wsztajerowski.journal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.Files.createTempFile;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

class MultiProducersMultiConsumersConcurrencyTest {
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
        BlockingQueue<Location> locationQueue = new LinkedBlockingQueue<>();
        AtomicInteger writesCounter = new AtomicInteger(0);
        AtomicInteger readsCounter = new AtomicInteger(0);
        AtomicInteger sum = new AtomicInteger(0);
        int iterations = 1_000;
        try (ExecutorService executor = newFixedThreadPool(5)) {
            executor.submit(createProducer(iterations, locationQueue, writesCounter));
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                futures.add(executor.submit(createConsumer(iterations, locationQueue, readsCounter, sum)));
            }
            for (Future<?> future : futures) {
                future.get(1, TimeUnit.SECONDS); // Blocks until the task is completed
            }
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        assertThat(sum)
            .hasValue(iterations*(iterations-1)/2);
    }

    private Runnable createConsumer(int iterations, BlockingQueue<Location> locationQueue, AtomicInteger readsCounter, AtomicInteger sum) {
        return () -> {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(32);
                while (readsCounter.incrementAndGet() < iterations) {
                    buffer.clear();
                    Location location = locationQueue.poll(100, TimeUnit.MILLISECONDS);
                    var variable = sut.read(buffer, location);
                    sum.addAndGet(variable.getInt());
                }
            } catch (Exception e){
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable createProducer(int iterations, Queue<Location> locationQueue, AtomicInteger writesCounter) {
        return () -> {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(4);
                int iteration;
                while ((iteration = writesCounter.incrementAndGet()) < iterations) {
                    buffer.clear();
                    buffer.putInt(iteration);
                    buffer.flip();
                    var location = sut.write(buffer);
                    locationQueue.offer(location);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
