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
        long begin = System.currentTimeMillis();
        String input = "/home/phiha/workspace/code/data/100MB";
        ExternalSort externalSort = new ExternalSort(4);
        externalSort.sort(input, "out");
//        List<String> lines = new ArrayList<>();
//        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), StandardCharsets.UTF_8))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                lines.add(line);
//                if (lines.size() == 2960000) {
//                    break;
//                }
//            }
//        }
////        System.out.println(lines.to);
//        long read = System.currentTimeMillis();
//        System.out.printf("Reading took %d ms\n", (read - begin));
//        lines.parallelStream().sorted().collect(Collectors.toList());
//        System.out.printf("Sorting took %d ms\n", System.currentTimeMillis() - read);
    }
}
