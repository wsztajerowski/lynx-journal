package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

public class Batch {
    private final ByteBuffer batchByteBuffer;
    private final Condition batchHasFlushedCondition;
    private volatile boolean isBatchFlushed = false;
    private final AtomicLong virtualPosition;

    public Batch(int batchSize, AtomicLong virtualPosition, Condition batchHasFlushedCondition) {
        this.virtualPosition = virtualPosition;
        batchByteBuffer = ByteBuffer.allocateDirect(batchSize);
        this.batchHasFlushedCondition = batchHasFlushedCondition;
    }

    public boolean isEmpty() {
        return batchByteBuffer.position() == 0;
    }
    public boolean hasRemaining(long numberOfBytesToWrite) {
        return batchByteBuffer.remaining() >= numberOfBytesToWrite;
    }

    public long write(ByteBuffer buffer) throws InterruptedException {
        long location = virtualPosition.getAndAdd(buffer.remaining());
        buffer.mark();
        batchByteBuffer.put(buffer);
        buffer.reset();
        return location;
    }

    public ByteBuffer writableBuffer() {
        return batchByteBuffer.flip();
    }

    public void clear() {
        batchByteBuffer.clear();
    }

    public Condition getBatchHasFlushedCondition() {
        return batchHasFlushedCondition;
    }

    public boolean isFull() {
        return batchByteBuffer.remaining() == 0; // we can return true for 80-90% full
    }

    void markBatchAsFlushed() {
        isBatchFlushed = true;
    }

    boolean hasBatchFlushed() {
        return isBatchFlushed;
    }

    public void resetFlushMark() {
        isBatchFlushed = false;
    }
}
