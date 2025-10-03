package pl.wsztajerowski;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

class MPSCFramework<REQ, RES> implements AutoCloseable {

    private final AtomicReference<InnerBatch<REQ, RES>> currentBatchReference = new AtomicReference<>(new InnerBatch<>());
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Consumer<Exchange<REQ, RES>[]> processor;
    private final AtomicBoolean consumerAlive = new AtomicBoolean(true);

    MPSCFramework(Consumer<Exchange<REQ, RES>[]> processor) {
        this.processor = processor;
    }

    public static <Q, S> MPSCFramework<Q, S> create(Consumer<Exchange<Q, S>[]> processor) {
        MPSCFramework<Q, S> mpscFramework = new MPSCFramework<>(processor);
        return mpscFramework.startConsumer();
    }

    public MPSCFramework<REQ, RES> startConsumer() {
        consumerExecutor.submit(() -> {
//            System.out.println("Starting consumer thread");
            while (consumerAlive.get()) {

                var bufferBatch = currentBatchReference.get();
                if (!bufferBatch.content.isEmpty()) {
//                    System.out.println("Consuming " + bufferBatch.content.size() + " buffers");
                    lock.writeLock().lock();
                    try {
//                        System.out.println("Finalizing batch");
                        bufferBatch.finalizeBatch();
                    } finally {
                        lock.writeLock().unlock();
                    }
//                    System.out.println("Change batch reference");
                    currentBatchReference.set(new InnerBatch<>());
//                    System.out.println("Processing " + bufferBatch.content.size() + " buffers");
                    processBatch(bufferBatch);
                }
            }
//            System.out.println("Consumer stopped");
        });
        return this;
    }

    private void processBatch(InnerBatch<REQ, RES> polledBatch) {
        Exchange<REQ, RES>[] batchContent = polledBatch.getBatchContent();
        processor.accept(batchContent);
        polledBatch.sendDoneSignal();
    }

    public RES produce(REQ request) {
        Exchange<REQ, RES> exchange = new Exchange<>(request);
        while (true) { //lock-free loop
            // cala zabawa z lockami
            var batch = currentBatchReference.get();
            var batchFinalized = batch.isBatchFinalized();

            if (!batchFinalized) {
                lock.readLock().lock();
                // <-- tu OS scheduler może odjebać
                CountDownLatch doneSignal;
                try {
                    batchFinalized = batch.isBatchFinalized();
                    if (batchFinalized || (doneSignal = batch.offer(exchange)) == null) {
                        continue;
                    }
                } finally {
                    lock.readLock().unlock();
                }
                // tu jestesmy juz poza lockiem
                try {
                    doneSignal.await();
                    return exchange.response;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        consumerAlive.set(false);
        consumerExecutor.shutdown();
        try {
            if (!consumerExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                consumerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            consumerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
