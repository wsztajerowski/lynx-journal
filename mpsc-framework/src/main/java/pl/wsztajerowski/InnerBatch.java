package pl.wsztajerowski;

import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class InnerBatch<REQ, RES> {
    static int BATCH_SIZE = 1024; // Define batch size
     final MpscArrayQueue<Wrapper<REQ, RES>> content;
     final CountDownLatch doneSignal;
    private volatile boolean batchFinalized;
    final AtomicBoolean isEmpty;

    InnerBatch() {
        this.doneSignal = new CountDownLatch(1);
        batchFinalized = false;
        content = new MpscArrayQueue<>(BATCH_SIZE);
        isEmpty = new AtomicBoolean(true);
    }

    public void finalizeBatch() {
        batchFinalized = true;
    }

    public void sendDoneSignal() {
        doneSignal.countDown();
    }

    public Wrapper<REQ, RES>[] getBatchContent() {
        return content.toArray(new Wrapper[0]);
    }

    public boolean isBatchFinalized() {
        return batchFinalized;
    }

    public CountDownLatch getDoneSignal() {
        return doneSignal;
    }

    public CountDownLatch offer(Wrapper<REQ, RES> content) {
        if (!batchFinalized && this.content.offer(content)) {
            isEmpty.compareAndExchange(true, false);
            return doneSignal;
        }
        batchFinalized = true;
        return null;
    }

    public boolean isBatchEmpty() {
        return isEmpty.get();
    }
}
