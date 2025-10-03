package pl.wsztajerowski;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MPSCFrameworkTest {

    public static final int REQUEST_NO = 10_000;

    @Test
    void runProducers() throws Exception {
        MPSCFramework<Integer, Integer> mpsc = getFramework();
        System.out.println("Consumer started");
        AtomicInteger atomicInteger = new AtomicInteger(0);
        try (ExecutorService producerPool = Executors.newFixedThreadPool(30)) {
            for (int i = 0; i < REQUEST_NO; i++) {
                int msgId = i;
                producerPool.submit(() -> {
                    Integer produced = mpsc.produce(msgId);
                    atomicInteger.addAndGet(produced);
//                    System.out.println("Produced " + msgId);
                });
            }
        }
        System.out.println("Waiting for producer to finish");
//        producerPool.shutdown();
//        producerPool.awaitTermination(50, TimeUnit.SECONDS);
//        System.out.println("Producer finished");
        mpsc.close();
        System.out.println("Consumer stopped");
        assertThat(atomicInteger.get())
            .isEqualTo(REQUEST_NO*(REQUEST_NO + 1)/2);
//        System.out.println("Consumer counter:" + atomicInteger.get());
    }

    private static MPSCFramework<Integer, Integer> getFramework() {
        MPSCFramework<Integer, Integer> mpsc = new MPSCFramework<>(wrappers -> {
            for (Exchange<Integer, Integer> wrapper : wrappers) {
//                System.out.println("Consumed: " + wrapper.request);
                wrapper.response = wrapper.request + 1;
            }
            try {
                Thread.sleep(5); // Simulate batch processing delay
            } catch (InterruptedException e) {
                System.out.println("Consumer Interrupted");
                Thread.currentThread().interrupt();
            }
        });

        mpsc.startConsumer();
        return mpsc;
    }
}