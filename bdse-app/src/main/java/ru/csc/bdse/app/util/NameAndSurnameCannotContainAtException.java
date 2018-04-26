package ru.csc.bdse.app.util;

public class NameAndSurnameCannotContainAtException extends RuntimeException {
    public NameAndSurnameCannotContainAtException() {
        super("Name and surname cannot contain '@' character.");
    }

    public NameAndSurnameCannotContainAtException(String message) {
        super(message);
    }
}
