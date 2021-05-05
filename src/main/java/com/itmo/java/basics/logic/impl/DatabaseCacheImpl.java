package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CASH_CAPACITY = 5000;
    private final LinkedHashMap<String, byte[]> cache;
    public DatabaseCacheImpl(int capacity)
    {
        cache = new LinkedHashMap<>(capacity){
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return size() > capacity;
            }
        };
    }

    public DatabaseCacheImpl()
    {
        this(CASH_CAPACITY);
    }


    @Override
    public byte[] get(String key) {
        return cache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }
}
