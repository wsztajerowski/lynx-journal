package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;

public class Batch {
    private final ByteBuffer batchByteBuffer;
    private final AtomicLong virtualPosition;
    private final Condition batchHasFlushedCondition;
    private final Condition batchIsFullCondition;

    public Batch(int batchSize, AtomicLong virtualPosition, Condition batchHasFlushedCondition, Condition batchIsFullCondition) {
        this.virtualPosition = virtualPosition;
        this.batchHasFlushedCondition = batchHasFlushedCondition;
        this.batchIsFullCondition = batchIsFullCondition;
        batchByteBuffer = ByteBuffer.allocateDirect(batchSize);
    }

    public boolean isEmpty() {
        return batchByteBuffer.position() == 0;
    }
    public boolean hasRemaining(long numberOfBytesToWrite) {
        return batchByteBuffer.remaining() >= numberOfBytesToWrite;
    }

    public long write(ByteBuffer buffer, boolean waitForFlush) throws InterruptedException {
        long location = virtualPosition.getAndAdd(buffer.remaining());
        buffer.mark();
        batchByteBuffer.put(buffer);
        buffer.reset();
        if (waitForFlush) {
            batchHasFlushedCondition.await();        // spurious wakeup ?
        }
        return location;
    }

//        int batchByteBufferPosition = batchByteBuffer.position();
//        try {
//
////            batchByteBuffer.position(batchByteBufferPosition + numberOfBytesToWrite);
//        } finally {
//        }
        // copy data outside lock
//        batchByteBuffer.put(batchByteBufferPosition, buffer, buffer.position(), numberOfBytesToWrite);
//        Condition condition = batchLock.writeLock().newCondition();     // To raczej nie tak...
//        return Optional.of(new WriteResult(location, condition));
//    }

    public ByteBuffer writableBuffer() {
        return batchByteBuffer.flip();
    }

    public void clear() {
        batchByteBuffer.clear();
    }

    public void notifyIsFull() {
        batchIsFullCondition.signal();
    }

    public void awaitOnIsFullCondition() {
        batchIsFullCondition.awaitUninterruptibly();
    }

    public void signalFlushedAll() {
        batchHasFlushedCondition.signalAll();
    }
}
