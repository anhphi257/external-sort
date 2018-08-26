package com.github.anhphi257.old;

import java.io.*;
import java.nio.file.Paths;
import java.security.SecureRandom;

public class Utils {
    public static String DATA_DIR = "data";
    public static String charSet = "abcdefghijklmnopqrstuvwxyz0123456789";

    private static void mkdir(String dir) {
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    public static void generateTestFile(String name, long size, long maxLineLength) throws IOException {
        mkdir(DATA_DIR);
        String path = Paths.get(DATA_DIR, name).toString();
        long currentSize = 0;
        SecureRandom random = new SecureRandom();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)))) {
            while (currentSize < size) {
                long len = random.nextLong() % maxLineLength;
                if (len == 0) len++;
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < len; i++) {
                    int index = random.nextInt(charSet.length());
                    builder.append(charSet.charAt(index));
                }
                if(builder.toString().length() > 0)
                writer.write(builder.toString() + "\n");
                currentSize += builder.length() + 1;
            }
        }

    }
}
