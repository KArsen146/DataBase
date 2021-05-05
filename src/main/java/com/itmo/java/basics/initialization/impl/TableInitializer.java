package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;
    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File dir = new File(context.currentTableContext().getTablePath().toString());
        if (!dir.exists()) {
            throw new DatabaseException(String.format("Directory with path %s is not exist", dir.getPath()));
        }
        if (!dir.canRead()){
            throw new DatabaseException(String.format("Error with reading files from directory with path %s",
                    dir.getPath()));
        }
        InitializationContextImpl.InitializationContextImplBuilder contextBuilder =
                InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(context.currentDbContext())
                        .currentTableContext(context.currentTableContext());
        File[] files = dir.listFiles();
        Arrays.sort(files);
        for (File item : files) {
            if (item.isFile()) {
                SegmentInitializationContextImpl segmentContext = new SegmentInitializationContextImpl(item.getName(), Paths.get(dir.toPath().toString(), item.getName()), 0, new SegmentIndex());
                InitializationContextImpl newContext = contextBuilder.currentSegmentContext(segmentContext).build();
                segmentInitializer.perform(newContext);
            }
        }
        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}