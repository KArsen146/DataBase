package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая удаление значения по ключу
 */
public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private static final int REMOVED_OBJECT_SIZE = -1;
    private final byte[] key;
    public RemoveDatabaseRecord(byte[] key)
    {
        this.key = key;
    }
    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    /**
     *
     * @return размер записи, где 8 - размер записи чисел, равных размерам ключа и значения( 2 размера Integer в байтах)
     */
    @Override
    public long size() {
        return key.length + 8;
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return REMOVED_OBJECT_SIZE;
    }
}
