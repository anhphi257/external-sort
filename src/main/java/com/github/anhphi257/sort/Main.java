package com.github.anhphi257.sort;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        String input = "data/100MB";
        ExternalSort externalSort = new ExternalSort(4);
        externalSort.sort(input, "out");
    }
}
