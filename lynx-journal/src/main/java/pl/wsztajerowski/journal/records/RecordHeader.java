package pl.wsztajerowski.journal.records;

import static pl.wsztajerowski.journal.records.InvalidRecordHeaderException.invalidRecordVariableSize;

public record RecordHeader(int variableSize, long checksum){
    // v02 record header format: [ int prefix, int variableSize, long checksum ]
    static final int RECORD_PREFIX = 0xF0CACC1A;
    private static final int NUMBER_OF_INTS_IN_HEADER = 2;

    public static int recordHeaderLength() {
        return NUMBER_OF_INTS_IN_HEADER * Integer.BYTES + Long.BYTES;
    }

    public static RecordHeader createAndValidateHeader(int variableSize, long checksum) {
        if (variableSize <= 0 ){
            throw invalidRecordVariableSize(variableSize);
        }
        return new RecordHeader(variableSize, checksum);
    }
}
