package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final byte[] key;
    private final byte[] value;
    public SetDatabaseRecord(byte[] key, byte[] value)
    {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    /**
     *
     * @return размер записи, где 8 - размер записи чисел, равных размерам ключа и значения( 2 размера Integer в байтах)
     */
    @Override
    public long size() {
        return getKeySize() + getValueSize() + 8;
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return value.length;
    }
}
