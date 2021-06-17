package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.List;

public class CommandReader implements AutoCloseable {
    private final RespReader reader;
    private final ExecutionEnvironment env;

    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.reader = reader;
        this.env = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
        RespArray array = reader.readArray();
        final List<RespObject> objects = array.getObjects();
        if (objects.size() < 3) {
            throw new IllegalArgumentException(String.format("Array %s has in correct size. Size must be above 2, but array size is %d", array.asString(), objects.size()));
        }

        if (objects.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).getClass() != RespCommandId.class) {
            throw new IllegalArgumentException("Array does not contain Command Id or it is out of position");
        }

        if (objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).getClass() != RespBulkString.class) {
            throw new IllegalArgumentException("Array does not contain Command Name or it is out of position");
        }
        return DatabaseCommands.valueOf(objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString())
                .getCommand(env, objects);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
