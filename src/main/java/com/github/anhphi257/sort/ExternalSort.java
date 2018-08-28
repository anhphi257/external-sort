package com.github.anhphi257.sort;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ExternalSort {
    private static String TMP_DIR = "tmp";
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
        this.queue = new ArrayBlockingQueue<>(numThreads);
        System.out.println("Number threads: " + this.numThreads);
        System.out.println("Block size: " + this.blockSize);
    }


    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        if (args.length < 2) {
            System.out.println("Usage: java -Xmx<MEM_SIZE> -cp external-sort-1.0-SNAPSHOT.jar com.github.anhphi257.sort.ExternalSort" +
                    " <input> <output> <numThreads>");
            System.exit(0);
        }
        String input = args[0];
        String output = args[1];

        int numThread = Integer.parseInt(args[2]);
        ExternalSort sort = new ExternalSort(numThread);
        sort.sort(input, output);
    }

    private long estimasteBlockSize() {
        System.gc();
        Runtime r = Runtime.getRuntime();
        long usedMem = r.totalMemory() - r.freeMemory();
        long freeMem = r.maxMemory() - usedMem;
        System.out.println("Free mem: " + freeMem);
        return freeMem / (numThreads * 16);
    }

    public void sort(String input, String output) throws IOException, ExecutionException, InterruptedException {
        sort(new File(input), new File(output));
    }

    public void sort(File input, File output) throws IOException, ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        List<File> files = splitAndSort(input);
        long now = System.currentTimeMillis();
        System.out.printf("split and sort took %dms\n", now - start);
        mergeSortedFiles(files, output);
        long end = System.currentTimeMillis();
        System.out.printf("merging took %dms\n", end - now);
        System.exit(0);
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
            File tmp = Paths.get(TMP_DIR, "merge" + fromIndex + "-" + toIndex).toFile(); 
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
                        System.out.println("Current used memory in heap: " + (runtime.totalMemory() - runtime.freeMemory()) + " bytes");
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
//                            System.out.println("Write done. Current queue size: " + queue.size());
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

        return files;
    }


    private static class Helper {

        static String createTemperaryDirectory() {
            String dirPath = Paths.get(TMP_DIR).toString();
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

        static void waitExecution(Future... futures) {
            for (Future f : futures) {
                waitExecution(f);
            }
        }

        static <T> T waitExecution(Future<T> future) {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }

        static void waitExecution(List<Future> futures) {
            waitExecution(futures.toArray(new Future[futures.size()]));
        }
    }

    final class Merger implements Runnable {
        private List<File> files;
        private File outFile;

        public Merger(List<File> files, File out) {
            this.files = files;
            this.outFile = out;
        }


        @Override
        public void run() {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), DEFAULT_CHARSET))) {
                PriorityQueue<FileCache> queue = new PriorityQueue<>(Comparator.comparing(FileCache::peek));
                for (File file : files) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), DEFAULT_CHARSET));
                    FileCache fileCache = new FileCache(reader);
                    if (!fileCache.empty())
                        queue.add(fileCache);
                }

                while (!queue.isEmpty()) {
                    FileCache cache = queue.poll();
                    writer.write(cache.poll());
                    writer.newLine();
                    if (!cache.empty()) {
                        queue.add(cache);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            for (File file : files) {
                file.delete();
            }
            System.out.println("Merge done");
        }
    }

    final class FileCache {
        private BufferedReader reader;
        private String cache;

        FileCache(BufferedReader reader) throws IOException {
            this.reader = reader;
            this.cache = reader.readLine();
        }

        String peek() {
            return this.cache;
        }

        String poll() throws IOException {
            String answer = this.cache;
            this.cache = reader.readLine();
            return answer;
        }

        boolean empty() {
            return this.cache == null;
        }
    }

}

