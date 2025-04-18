package pl.wsztajerowski;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

class MPSCFramework<REQ, RES> implements AutoCloseable {

    private volatile InnerBatch<REQ, RES> currentBatchReference = new InnerBatch<>();
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Consumer<Wrapper<REQ, RES>[]> processor;
    private volatile boolean consumerAlive = true;

    MPSCFramework(Consumer<Wrapper<REQ, RES>[]> processor) {
        this.processor = processor;
    }

    public static <Q, S> MPSCFramework<Q, S> create(Consumer<Wrapper<Q, S>[]> processor) {
        MPSCFramework<Q, S> mpscFramework = new MPSCFramework<>(processor);
        return mpscFramework.startConsumer();
    }

    public MPSCFramework<REQ, RES> startConsumer() {
        consumerExecutor.submit(() -> {
            while (consumerAlive) {
                var bufferBatch = currentBatchReference;
                if (!bufferBatch.isBatchEmpty()) { // a co jesli empty?
                    lock.writeLock().lock();
                    try {
                        bufferBatch.finalizeBatch();
                    } finally {
                        lock.writeLock().unlock();
                    }
                    currentBatchReference = new InnerBatch<>();
                    processBatch(bufferBatch);
                } else{
                    Thread.onSpinWait();
                }
            }
        });
        return this;
    }

    private void processBatch(InnerBatch<REQ, RES> polledBatch) {
        Wrapper<REQ, RES>[] batchContent = polledBatch.getBatchContent();
        processor.accept(batchContent);
        polledBatch.sendDoneSignal();
    }

    public RES produce(REQ request) {
        Wrapper<REQ, RES> wrapper = new Wrapper<>(request);
        while (true) { //lock-free loop
            // cala zabawa z lockami
            var batch = currentBatchReference;

            if (!batch.isBatchFinalized()) {
                lock.readLock().lock();
                // <-- tu OS scheduler może odjebać
                CountDownLatch doneSignal;
                try {
                    if (batch.isBatchFinalized() || (doneSignal = batch.offer(wrapper)) == null) {
                        continue;
                    }
                } finally {
                    lock.readLock().unlock();
                }
                // tu jestesmy juz poza lockiem
                try {
                    doneSignal.await();
                    return wrapper.response;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        consumerAlive = false;
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

    public static void main(String[] args) {
        MPSCFramework<Integer, Integer> mpsc = MPSCFramework.create(wrappers -> {
            for (Wrapper<Integer, Integer> wrapper : wrappers) {
                System.out.println("Consumed: " + wrapper.request);
                wrapper.response = wrapper.request + 1;
            }
        });
        mpsc.startConsumer();
    }
}
