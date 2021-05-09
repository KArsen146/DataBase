package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String name;
    private final Path path;
    private final SegmentIndex index;
    private final long size;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
        name = segmentName;
        path = segmentPath;
        size = currentSize;
        this.index = index;
    }


    /**
     * Не используйте этот конструктор. Оставлен для совместимости со старыми тестами.
     */
    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {
        this(segmentName, tablePath.resolve(segmentName), currentSize, new SegmentIndex());
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath) {
        this(segmentName, tablePath.resolve(segmentName), 0, new SegmentIndex());
    }

    @Override
    public String getSegmentName() {
        return name;
    }

    @Override
    public Path getSegmentPath() {
        return path;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }

    @Override
    public long getCurrentSize() {
        return size;
    }
}
