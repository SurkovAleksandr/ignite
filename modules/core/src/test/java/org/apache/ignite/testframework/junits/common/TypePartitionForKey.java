package org.apache.ignite.testframework.junits.common;

/** Enum describe different relationship between partition and key. */
public enum TypePartitionForKey {
    // Partition is primary for key.
    PRIMARY(0),
    // Partition is backup for key.
    BACKUP(1),
    // Partition not primary and not backup for key.
    OTHER(2);

    private int keyIndex;

    TypePartitionForKey(int keyIndex) {
        this.keyIndex = keyIndex;
    }

    public int getKeyIndex() {
        return keyIndex;
    }
}
