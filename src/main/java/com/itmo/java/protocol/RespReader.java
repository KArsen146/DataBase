package com.itmo.java.protocol;

import com.itmo.java.protocol.model.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private final PushbackInputStream is;

    public RespReader(InputStream is) {
        this.is = new PushbackInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return getFirstByte() == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        switch (getFirstByte()) {
            case RespArray.CODE:
                return readArray();
            case RespBulkString.CODE:
                return readBulkString();
            case RespCommandId.CODE:
                return readCommandId();
            case RespError.CODE:
                return readError();
            default:
                throw new IOException("Incorrect RespObject");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        if (readByte() != RespError.CODE) {
            throw new IOException("RespObject is not RespError");
        }
        return new RespError(readBeforeSeparator());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        if (readByte() != RespBulkString.CODE) {
            throw new IOException("RespObject is not RespBulkString");
        }
        int length = Integer.parseInt(new String(readBeforeSeparator()));
        if (length == -1) {
            return RespBulkString.NULL_STRING;
        }
        byte[] payload = is.readNBytes(length);
        checkFinal();
        return new RespBulkString(payload);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        if (readByte() != RespArray.CODE) {
            throw new IOException("RespObject is not RespArray");
        }
        int length = Integer.parseInt(new String(readBeforeSeparator()));
        if (length < 1) {
            throw new IOException(String.format("Incorrect RespArray size %d", length));
        }
        RespObject[] objects = new RespObject[length];
        for (int i = 0; i < length; i++) {
            objects[i] = readObject();
        }
        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        if (readByte() != RespCommandId.CODE) {
            throw new IOException("RespObject is not RespCommandId");
        }
        int id = readInt();
        checkFinal();
        return new RespCommandId(id);
    }

    private int readInt() throws IOException {
        int ch1 = is.read();
        int ch2 = is.read();
        int ch3 = is.read();
        int ch4 = is.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    private void checkFinal() throws IOException {
        byte[] bytes = new byte[2];
        if (is.read(bytes) != 2) {
            throw new IOException("InputStream is empty");
        }
        if ((bytes[0] != CR) || (bytes[1] != LF)) {
            throw new IOException("Incorrect object: illegal end of stream characters");
        }
    }

    private byte getFirstByte() throws IOException {
        byte b = readByte();
        is.unread(b);
        return b;
    }

    private byte readByte() throws IOException {
        byte b = (byte) is.read();
        if (b == -1) {
            throw new IOException("InputStream is empty");
        }
        return b;
    }

    private byte[] readBeforeSeparator() throws IOException {
        ArrayList<Byte> bytes = new ArrayList<>();
        boolean flag = false;
        while (true) {
            byte b = readByte();
            if ((flag) && (b == LF)) {
                bytes.remove(bytes.size() - 1);
                break;
            }
            flag = b == CR;
            bytes.add(b);
        }
        byte[] arrayByte = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) {
            arrayByte[i] = bytes.get(i);
        }
        return arrayByte;
    }

    @Override
    public void close() throws IOException {
        is.close();
    }
}
