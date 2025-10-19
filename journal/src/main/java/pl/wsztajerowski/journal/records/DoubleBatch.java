package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DoubleBatch {
    public static final int BATCH_SIZE = 4096;
    private final Batch batchA;
    private final Batch batchB;
    private final AtomicReference<Batch> currentBatchReference;
    private final ReentrantReadWriteLock referenceLock;
    private final AtomicLong virtualPosition;

    public DoubleBatch() {
        virtualPosition = new AtomicLong(0);
        batchA = new Batch(BATCH_SIZE, virtualPosition);
        batchB = new Batch(BATCH_SIZE, virtualPosition);
        currentBatchReference = new AtomicReference<>(batchA);
        referenceLock = new ReentrantReadWriteLock();
    }

    Batch getCurrentBatch() {
        return currentBatchReference.get();
    }

    public Optional<WriteResult> write(ByteBuffer buffer) {
        referenceLock.readLock().lock();
        try {
            return currentBatchReference.get().write(buffer);
        } finally {
            referenceLock.readLock().unlock();
        }
    }

    void switchBatch() {
        referenceLock.writeLock().lock();
        try {
            currentBatchReference.getAndSet()set(currentBatchReference.get() == batchA ? batchB : batchA);
        } finally {
            referenceLock.writeLock().unlock();
        }
    }

    public void initVirtualPosition(long initJournalFilePosition) {
        this.virtualPosition.set(initJournalFilePosition);
    }
}
