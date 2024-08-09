package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;

import java.nio.ByteBuffer;

import static pl.wsztajerowski.journal.records.ChecksumCalculator.computeChecksum;

public record Record(RecordHeader recordHeader, Location location, ByteBuffer buffer) {
    public static Record createAndValidateRecord(RecordHeader recordHeader, Location location, ByteBuffer buffer) {
        var calculatedChecksum = computeChecksum(buffer);
        if (calculatedChecksum != recordHeader.checksum()) {
            throw new InvalidRecordChecksumException(calculatedChecksum, recordHeader.checksum());
        }
        return new Record(recordHeader, location, buffer);
    }
}
