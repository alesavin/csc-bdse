package ru.csc.bdse.util;

public class IllegalNodeStateException extends RuntimeException {
    public IllegalNodeStateException() {
        super();
    }

    public IllegalNodeStateException(String message) {
        super(message);
    }
}
