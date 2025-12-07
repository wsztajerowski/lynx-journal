package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

class Batch {
    private final ByteBuffer batchBuffer;
    private final Condition hasFlushedCondition;
    private volatile boolean isFlushed = false;
    private final AtomicLong virtualPosition;

    Batch(int batchSize, AtomicLong virtualPosition, Condition hasFlushedCondition) {
        this.virtualPosition = virtualPosition;
        this.hasFlushedCondition = hasFlushedCondition;
        batchBuffer = ByteBuffer.allocateDirect(batchSize);
    }

    boolean isEmpty() {
        return batchBuffer.position() == 0;
    }

    boolean hasRemaining(long numberOfBytesToWrite) {
        return batchBuffer.remaining() >= numberOfBytesToWrite;
    }

    long write(ByteBuffer buffer) throws InterruptedException {
        long location = virtualPosition.getAndAdd(buffer.remaining());
        buffer.mark();
        batchBuffer.put(buffer);
        buffer.reset();
        return location;
    }

    ByteBuffer writableBuffer() {
        return batchBuffer.flip();
    }

    void clear() {
        batchBuffer.clear();
    }

    Condition getHasFlushedCondition() {
        return hasFlushedCondition;
    }

    boolean isFull() {
        return batchBuffer.remaining() == 0; // we can return true for 80-90% full
    }

    void markBatchAsFlushed() {
        isFlushed = true;
    }

    boolean hasBatchFlushed() {
        return isFlushed;
    }

    void resetFlushMark() {
        isFlushed = false;
    }
}
