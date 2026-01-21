package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

class IOWritesBatch {
    private final ByteBuffer batchBuffer;
    private final Condition isFlushedCondition;
    private volatile boolean isFlushed = false;
    private final AtomicLong virtualFileChannelPosition;

    IOWritesBatch(ByteBuffer batchBuffer, AtomicLong virtualFileChannelPosition, Condition isFlushedCondition) {
        this.batchBuffer = batchBuffer;
        this.virtualFileChannelPosition = virtualFileChannelPosition;
        this.isFlushedCondition = isFlushedCondition;
    }

    boolean isEmpty() {
        return batchBuffer.position() == 0;
    }

    boolean hasRemaining(long numberOfBytesToWrite) {
        return batchBuffer.remaining() >= numberOfBytesToWrite;
    }

    long write(ByteBuffer buffer) {
        long location = virtualFileChannelPosition.getAndAdd(buffer.remaining());
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

    boolean isFull() {
        return batchBuffer.remaining() == 0; // we can return true for 80-90% full
    }

    void markBatchAsFlushed() {
        isFlushed = true;
    }

    boolean isBatchFlushed() {
        return isFlushed;
    }

    void resetFlushMark() {
        isFlushed = false;
    }

    public void await() throws InterruptedException {
        isFlushedCondition.await();
    }

    public void signalAll() {
        isFlushedCondition.signalAll();
    }
}
