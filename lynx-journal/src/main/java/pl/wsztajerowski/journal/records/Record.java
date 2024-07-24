package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;

import java.nio.ByteBuffer;

public record Record(RecordHeader recordHeader, Location location, ByteBuffer buffer) {
}
