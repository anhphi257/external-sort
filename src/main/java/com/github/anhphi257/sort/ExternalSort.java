package com.github.anhphi257.sort;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExternalSort {
    private static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static Comparator<String> DEFAULT_COMPARATOR = Comparator.naturalOrder();
    private int numThreads;
    private long blockSize;
    private boolean readDone;
    private BlockingQueue<List<String>> queue;


    public ExternalSort() {
        new ExternalSort(1);
    }

    public ExternalSort(int numThreads) {
        this.numThreads = numThreads;
        this.blockSize = estimasteBlockSize();
        this.queue = new ArrayBlockingQueue<>(4);
        System.out.println("Number threads: " + this.numThreads);
        System.out.println("Block size: " + this.blockSize);
    }

    private long estimasteBlockSize() {
        System.gc();
        Runtime r = Runtime.getRuntime();
        long usedMem = r.totalMemory() - r.freeMemory();
        long freeMem = r.maxMemory() - usedMem;
        System.out.println("Free mem: " + freeMem);
        return freeMem / (numThreads * 16 + 1);
    }

    public void sort(String input, String output) throws IOException, ExecutionException, InterruptedException {
        sort(new File(input), new File(output));
    }

    public void sort(File input, File output) throws IOException, ExecutionException, InterruptedException {
        List<File> files = splitAndSort(input);
        mergeSortedFiles(files, output);
    }

    //2-passes merging
    private void mergeSortedFiles(List<File> files, File output) throws InterruptedException {
        System.out.println("Start merging");
        //first pass

        List<File> firstPassMergeFiles = new ArrayList<>();
        int batch = (int) Math.sqrt(files.size());
        System.out.println("First phase with batch: " + batch);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future> firstPassMergeFutures = new ArrayList<>();
        int fromIndex = 0;
        while (fromIndex < files.size()) {
            int toIndex = Math.min(fromIndex + batch, files.size());
            File tmp = Paths.get("..", "tmp2", fromIndex + "-" + toIndex).toFile(); //TODO: implement output path
            Future<?> future = executor.submit(new Merger(files.subList(fromIndex, toIndex), tmp));
            firstPassMergeFiles.add(tmp);
            firstPassMergeFutures.add(future);
            fromIndex = toIndex;
        }
        Helper.waitExecution(firstPassMergeFutures);
        //second pass
        Thread.sleep(5000);
        System.out.println("Second phase merging");
        ExecutorService secondPassMergeExecutor = Executors.newFixedThreadPool(1);
        Future<?> secondPassFuture = secondPassMergeExecutor.submit(new Merger(firstPassMergeFiles, output));
        Helper.waitExecution(secondPassFuture);

    }


    private List<File> splitAndSort(File input) throws IOException, InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        AtomicInteger numFile = new AtomicInteger();
        String tmpDir = Helper.createTemperaryDirectory();
        List<File> files = new ArrayList<>();
        ExecutorService readerPool = Executors.newFixedThreadPool(1);

        Future<Boolean> readerFuture = readerPool.submit(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), DEFAULT_CHARSET))) {
                List<String> lines = new ArrayList<>();
                long currentSize = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    int length = line.getBytes(DEFAULT_CHARSET).length;
                    if (length + currentSize < blockSize) {
                        lines.add(line);
                        currentSize += length;
                    } else {
                        Runtime runtime = Runtime.getRuntime();
                        System.out.println("Add to queue " + lines.size() + " lines");
                        System.out.println(runtime.totalMemory() - runtime.freeMemory());
                        queue.put(lines);
                        numFile.incrementAndGet();
                        lines = new ArrayList<>();
                        System.gc();
                        lines.add(line);
                        currentSize = length;
                    }
                }

                if (lines.size() > 0) {
                    queue.put(lines);
                    numFile.incrementAndGet();
                    System.gc();

                }
            }
            System.out.println("READ DONE");

            return Boolean.TRUE;
        });

        AtomicInteger fileIndex = new AtomicInteger(0);
        List<Future> sorterFutures = new ArrayList<>();
        ExecutorService sorterPool = Executors.newFixedThreadPool(this.numThreads);
        for (int i = 0; i < numThreads; i++) {
            sorterFutures.add(sorterPool.submit(() -> {
                while (true) {
                    List<String> lines;
                    lines = queue.poll(50l, TimeUnit.MILLISECONDS);
                    if (lines != null) {
                        lines = lines.parallelStream().sorted(DEFAULT_COMPARATOR).collect(Collectors.toList());
                        File file = Paths.get(tmpDir, input.getName() + "-" + fileIndex.getAndIncrement()).toFile();
                        System.out.println("Start sorting and writing to file: " + file.getName());
                        try (BufferedWriter writer = new BufferedWriter(
                                new OutputStreamWriter(new FileOutputStream(file, false), DEFAULT_CHARSET))) {
                            for (String line : lines) {
                                writer.write(line);
                                writer.newLine();
                            }
                            numFile.decrementAndGet();
                            System.gc();
                            files.add(file);
                            System.out.println("Write done. Current queue size: " + queue.size());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    if (readDone && numFile.get() == 0) {
                        return Boolean.TRUE;
                    }
                }
            }));
        }

        readDone = Helper.waitExecution(readerFuture);
        Helper.waitExecution(sorterFutures);

        long end = System.currentTimeMillis();
        System.out.println("Split and sort in: " + (end - start) + " ms");
        return files;
    }


    private static class Helper {

        public static String createTemperaryDirectory() {
            String dirPath = Paths.get("E:\\code", "tmp").toString();
            File file = new File(dirPath);
            if (file.exists()) {
                if (file.isDirectory()) {
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
