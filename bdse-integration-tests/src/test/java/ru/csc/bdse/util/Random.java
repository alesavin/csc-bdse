package ru.csc.bdse.util;

/**
 * @author semkagtn
 */
public class Random {

    private Random() {

    }

    private static final java.util.Random random = new java.util.Random();

    public static String nextKey() {
        return String.valueOf(random.nextLong());
    }

    public static byte[] nextValue() {
        return nextKey().getBytes();
    }

    public static char nextChar() {
        return (char)(random.nextInt(26) + 'a');
    }
}
