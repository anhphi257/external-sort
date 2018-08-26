package com.github.anhphi257.old;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        Utils.generateTestFile("100MB", 1l * 500 * 1024 * 1024, 30);
    }
}
