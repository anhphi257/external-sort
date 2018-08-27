package com.github.anhphi257.old;

public class Test {
    public static void main(String[] args)   {
        int fromIndex = 0;
        int batch = 15;
        int size = 252;
        while (fromIndex  < size) {
            int toIndex = Math.min(fromIndex + batch, size);
            System.out.println(fromIndex + " - " + toIndex);
            fromIndex = toIndex;
        }
    }
}
