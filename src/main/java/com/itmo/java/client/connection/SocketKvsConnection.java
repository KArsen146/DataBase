package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final Socket socket;
    private final RespReader respReader;
    private final RespWriter respWriter;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            socket = new Socket(config.getHost(), config.getPort());
            respReader = new RespReader(socket.getInputStream());
            respWriter = new RespWriter(socket.getOutputStream());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            respWriter.write(command);
            return respReader.readObject();
        } catch (IOException e) {
            throw new ConnectionException(e.getMessage());
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            respWriter.close();
            respReader.close();
            socket.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
