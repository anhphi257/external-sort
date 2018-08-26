package com.github.anhphi257.sort;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        String input = "data/100MB";
        ExternalSort externalSort = new ExternalSort(4);
        externalSort.sort(input, "out");
    }
}
