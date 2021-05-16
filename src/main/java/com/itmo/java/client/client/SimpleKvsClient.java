package com.itmo.java.client.client;


import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String databaseName;
    private final KvsConnection connection;

    /**
     * Констурктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания коннекшена к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        connection = connectionSupplier.get();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        return sendCommand(new CreateDatabaseKvsCommand(databaseName));
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        return sendCommand(new CreateTableKvsCommand(databaseName, tableName));
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        return sendCommand(new GetKvsCommand(databaseName, tableName, key));
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        return sendCommand(new SetKvsCommand(databaseName, tableName, key, value));
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        return sendCommand(new DeleteKvsCommand(databaseName, tableName, key));
    }

    private String sendCommand(KvsCommand command) throws DatabaseExecutionException {
        try {
            RespObject object = connection.send(command.getCommandId(), command.serialize());
            if (object.isError()) {
                throw new DatabaseExecutionException(object.asString());
            }
            return object.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("Error with connection: %s", e.getMessage()));
        }
    }
}
