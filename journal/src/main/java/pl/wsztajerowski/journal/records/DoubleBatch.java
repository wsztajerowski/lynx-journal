package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DoubleBatch {
    private final Batch batchA;
    private final Batch batchB;
    private volatile Batch currentBatch;
    private final ReentrantLock batchLock;
    private final AtomicLong virtualPosition;

    public DoubleBatch(int batchSize) {
        virtualPosition = new AtomicLong(0);
        batchLock = new ReentrantLock();
        Condition batchAHasFlushedCondition = batchLock.newCondition();
        Condition batchBHasFlushedCondition = batchLock.newCondition();
        Condition batchAIsFullCondition = batchLock.newCondition();
        Condition batchBIsFullCondition = batchLock.newCondition();
        batchA = new Batch(batchSize, virtualPosition, batchAHasFlushedCondition, batchAIsFullCondition);
        batchB = new Batch(batchSize, virtualPosition, batchBHasFlushedCondition, batchBIsFullCondition);
        currentBatch = batchA;
    }

    public long write(ByteBuffer buffer, boolean waitForFlush) {
        batchLock.lock();
        try {
            if (!currentBatch.hasRemaining(buffer.remaining())){
                Batch oldBatch = currentBatch;
                currentBatch = swapBatch();
                oldBatch.notifyIsFull();
            }
            return currentBatch.write(buffer, waitForFlush);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            batchLock.unlock();
        }
    }

    private Batch swapBatch() {
        return currentBatch == batchA ? batchB : batchA;
    }

    public void initVirtualPosition(long initJournalFilePosition) {
        this.virtualPosition.set(initJournalFilePosition);
    }

    public Batch getCurrentBatch() {
        return currentBatch;
    }
}
