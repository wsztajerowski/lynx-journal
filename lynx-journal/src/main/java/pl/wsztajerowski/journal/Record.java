package pl.wsztajerowski.journal;

import java.nio.ByteBuffer;

public record Record(RecordHeader recordHeader, ByteBuffer buffer) {
}
