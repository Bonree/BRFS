package com.bonree.brfs.client;

import java.io.Closeable;
import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main(String[] args) {
        try(Closeable c = getException()) {
            System.out.println(c);
        } catch (Exception e) {
            System.out.println("ERROR");
            e.printStackTrace();
        }
    }
    
    private static Closeable get() {
        return () -> System.out.println("close");
    }
    
    private static Closeable getNull() {
        return null;
    }
    
    private static Closeable getException() {
        throw new RuntimeException();
    }
}
