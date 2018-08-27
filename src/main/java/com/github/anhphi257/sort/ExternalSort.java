package com.github.anhphi257.sort;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExternalSort {
    private static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static Comparator<String> DEFAULT_COMPARATOR = Comparator.naturalOrder();
    private static AtomicInteger atomicInteger = new AtomicInteger(0);
    private int numThreads;
    private long blockSize;
    private boolean readDone;
    private ArrayBlockingQueue<List<String>> queue;

    public ExternalSort() {
        new ExternalSort(1);
    }

    public ExternalSort(int numThreads) {
        this.numThreads = numThreads;
        this.blockSize = estimasteBlockSize();
        this.queue = new ArrayBlockingQueue<>(numThreads);
        System.out.println("Number threads: " + this.numThreads);
        System.out.println("Block size: " + this.blockSize);
    }

    private long estimasteBlockSize() {
        System.gc();
        Runtime r = Runtime.getRuntime();
        long usedMem = r.totalMemory() - r.freeMemory();
        long freeMem = r.maxMemory() - usedMem;
        System.out.println("Free mem: " + freeMem);
        return freeMem / (numThreads * 8 + 1);
    }

    public void sort(String input, String output) throws IOException, ExecutionException, InterruptedException {
        sort(new File(input), new File(output));
    }

    public void sort(File input, File output) throws IOException, ExecutionException, InterruptedException {
        List<File> files = splitAndSort(input);

    }


    private List<File> splitAndSort(File input) throws IOException, InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();

        String tmpDir = Helper.createTemperaryDirectory();
        List<File> files = new ArrayList<>();
        ExecutorService fileReaderExecutor = Executors.newFixedThreadPool(1);
        Future<Boolean> readerSubmit = fileReaderExecutor.submit(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), DEFAULT_CHARSET))) {
                List<String> lines = new ArrayList<>();
                long currentSize = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    int length = line.getBytes().length; //Unicode 2 bytes per character
                    if (length + currentSize < blockSize) {
                        lines.add(line);
                        currentSize += length;
                    } else {
                        queue.put(lines);
                        lines = new ArrayList<>();
                        lines.add(line);
                        currentSize = 0;
                    }
                }

                if (lines.size() > 0) {
                    queue.put(lines);
                }
            }
            return Boolean.TRUE;
        });

        fileReaderExecutor.shutdown();

        List<Future> sorterFutures = new ArrayList<>();
        ExecutorService sorterPool = Executors.newFixedThreadPool(this.numThreads);
        for (int i = 0; i < numThreads; i++) {
            sorterFutures.add(sorterPool.submit(() -> {
                while (!readDone) {
                    List<String> lines = queue.take();

                    lines = lines.parallelStream().sorted(DEFAULT_COMPARATOR).collect(Collectors.toList());
                    int index = atomicInteger.getAndIncrement();
                    File file = Paths.get(tmpDir, input.getName() + "-" + index).toFile();
                    System.out.println("Start sorting and writing to file: " + file.getName());
                    try (BufferedWriter writer = new BufferedWriter(
                            new OutputStreamWriter(new FileOutputStream(file, false), DEFAULT_CHARSET))) {
                        for (String line : lines) {
                            writer.write(line);
                            writer.newLine();
                        }
                    }
                    files.add(file);
                }
                System.gc();
                return Boolean.TRUE;
            }));
        }
        sorterPool.shutdown();
        readDone = Helper.waitExecution(readerSubmit);
        Helper.waitExecution(sorterFutures);
        long end = System.currentTimeMillis();
        System.out.println("Split and sort in: " + (end - start) + " ms");
        return files;
    }

    private static class Helper {

        public static String createTemperaryDirectory() {
            String dirPath = Paths.get("/home/phiha/workspace/code/data/", "tmp").toString();
            File file = new File(dirPath);
            if (file.exists()) {
                if(file.isDirectory()) {
                    for (File file1 : file.listFiles()) {
                        file1.delete();
                    }
                }
                file.delete();
            }

            file.mkdir();
            return dirPath;
        }

        public static void waitExecution(Future... futures) {
            for (Future f : futures) {
                waitExecution(f);
            }
        }

        public static <T> T waitExecution(Future<T> future) {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }

        public static void waitExecution(List<Future> futures) {
            waitExecution(futures.toArray(new Future[futures.size()]));
        }
    }
}
