package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
    private final String name;
    private final Path path;
    private final SegmentIndex index;
    private final long size;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        name = segmentName;
        path = segmentPath;
        size = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this(segmentName, Paths.get(tablePath.toString(), segmentName), currentSize, null);
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
