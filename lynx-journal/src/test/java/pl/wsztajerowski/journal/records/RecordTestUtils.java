package pl.wsztajerowski.journal.records;

import static java.lang.Integer.toHexString;
import static pl.wsztajerowski.journal.records.RecordHeader.RECORD_PREFIX;

public class RecordTestUtils {

    public static CharSequence recordHeaderPrefixInHexString() {
        return toHexString(RECORD_PREFIX).toUpperCase();
    }
}
