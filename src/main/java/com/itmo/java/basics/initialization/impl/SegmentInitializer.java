package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path path = context.currentSegmentContext().getSegmentPath();
        long offset = 0;
        Set<String> keys = new HashSet<>();
        try (DatabaseInputStream stream = new DatabaseInputStream(new FileInputStream(path.toString()))) {
            Optional<DatabaseRecord> record = stream.readDbUnit();
            while (record.isPresent()){
                String key = new String(record.get().getKey(), StandardCharsets.UTF_8);
                keys.add(key);
                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(key,
                        new SegmentOffsetInfoImpl(offset));
                offset += record.get().size();
                record = stream.readDbUnit();
            }
            SegmentInitializationContext segmentContext =
                    new SegmentInitializationContextImpl(context.currentSegmentContext().getSegmentName(),
                            context.currentSegmentContext().getSegmentPath(),
                            (int) offset, context.currentSegmentContext().getIndex());
            Segment segment = SegmentImpl.initializeFromContext(segmentContext);
            for (String key: keys) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, segment);
            }
            context.currentTableContext().updateCurrentSegment(segment);
        }
        catch (FileNotFoundException e){
            throw new DatabaseException(String.format("File with name %s is not found",
                    context.currentSegmentContext().getSegmentName()));
        }
        catch (IOException e){
            throw new DatabaseException(String.format("Error with opening file with name %s",
                    context.currentSegmentContext().getSegmentName()));
        }
    }
}