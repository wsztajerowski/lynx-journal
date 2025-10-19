package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Batch {
    private final ByteBuffer batchByteBuffer;
    private final ReentrantReadWriteLock batchLock;
    private final AtomicLong virtualPosition;

    public Batch(int batchSize, AtomicLong virtualPosition) {
        this.virtualPosition = virtualPosition;
        batchLock = new ReentrantReadWriteLock();
        batchByteBuffer = ByteBuffer.allocateDirect(batchSize);
    }

    public boolean isEmpty() {
        return batchByteBuffer.position() == 0;
    }

    public Optional<WriteResult> write(ByteBuffer buffer) {
        int numberOfBytesToWrite = buffer.remaining();
        long location;

        batchLock.writeLock().lock();
        int batchByteBufferPosition = batchByteBuffer.position();
        try {
            if (batchByteBuffer.remaining() < numberOfBytesToWrite) {
                return Optional.empty();
            }
            location = virtualPosition.addAndGet(numberOfBytesToWrite);
            batchByteBuffer.position(batchByteBufferPosition + numberOfBytesToWrite);
//            buffer.mark();
//            batchByteBuffer.put(buffer);
//            buffer.reset();
        } finally {
            batchLock.writeLock().unlock();
        }
        // copy data outside lock
        batchByteBuffer.put(batchByteBufferPosition, buffer, buffer.position(), numberOfBytesToWrite);
        Condition condition = batchLock.writeLock().newCondition();     // To raczej nie tak...
        return Optional.of(new WriteResult(location, condition));
    }

    public ByteBuffer writableBuffer() {
        return batchByteBuffer.flip();
    }

    public void clear() {
        batchByteBuffer.clear();
    }
}
