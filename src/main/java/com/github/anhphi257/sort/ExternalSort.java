package com.github.anhphi257.sort;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ExternalSort {
    private static Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static Comparator<String> DEFAULT_COMPARATOR = Comparator.naturalOrder();
    private int numThreads;
    private ExecutorService executor;
    private long blockSize;

    public ExternalSort() {
        new ExternalSort(1);
    }

    public ExternalSort(int numThreads) {
        this.numThreads = numThreads;
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.blockSize = estimasteBlockSize();
        System.out.println("Number threads: " + this.numThreads);
        System.out.println("Block size: " + this.blockSize);
    }

    private long estimasteBlockSize() {
        System.gc();
        Runtime r = Runtime.getRuntime();
        long usedMem = r.totalMemory() - r.freeMemory();
        long freeMem = r.maxMemory() - usedMem;
        return freeMem / (numThreads   + 1);
    }

    public void sort(String input, String output) throws IOException, ExecutionException, InterruptedException {
        sort(new File(input), new File(output));
    }

    public void sort(File input, File output) throws IOException, ExecutionException, InterruptedException {
        List<File> files = splitAndSort(input);
    }

    private void sortAndSave(List<String >lines, File outFile) throws IOException {
        System.out.println("Start sort and write to file: " + outFile.getName());
        System.out.println(lines.size());
        List<String> sortedLines = lines.parallelStream().sorted(DEFAULT_COMPARATOR).collect(Collectors.toList());
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile, false), DEFAULT_CHARSET))) {
            for (String line : sortedLines) {
                if (line.length() > 0) {
                    writer.write(line);
                    writer.newLine();
                }
            }
        }
    }

    private List<File> splitAndSort(File input) throws IOException {

        String tmpDir = Helper.createTemperaryDirectory();
        List<File> files = new ArrayList<>();
        List<Future> futures = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input)))) {
            List<String> lines = new ArrayList<>();
            long currentSize = 0;
            int index = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                int length = line.length() * 2; //Unicode 2 bytes per character
                if (length + currentSize < blockSize) {
                    lines.add(line);
                    currentSize += length;
                } else {
                    String fileName = Paths.get(tmpDir, input.getName() + "-" + index).toString();
                    File f = new File(fileName);
                    futures.add(executor.submit(new Sorter(lines, f)));
//                    sortAndSave(lines, f);
                    files.add(f);
                    index++;
                    lines = new ArrayList<>();
                    lines.add(line);
                    currentSize = 0;

                }
            }

            if (lines.size() > 0) {
                String fileName = Paths.get(tmpDir, input.getName() + "-" + index).toString();
                File f = new File(fileName);
//                sortAndSave(lines, f);
                futures.add(executor.submit(new Sorter(lines, f)));
                files.add(f);
            }

        }
        executor.shutdown();
//        Helper.waitExecution(futures);

        return files;
    }

//    private static class Sorter implements Callable<Boolean> {
    private static class Sorter implements Runnable {

        private List<String> lines;
        private File outFile;

        public Sorter(List<String> lines, File outFile) {
            this.lines = lines;
            this.outFile = outFile;
        }

        @Override
        public void run()   {
            System.out.println("Start sort and write to file: " + outFile.getName());
            System.out.println(lines.size());
            List<String> sortedLines = lines.stream().sorted(DEFAULT_COMPARATOR).collect(Collectors.toList());
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile, false), DEFAULT_CHARSET))) {
                for (String line : sortedLines) {
                    if (line.length() > 0) {
                        writer.write(line);
                        writer.newLine();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
//            return true;
        }
    }

    private static class Helper {

        public static String createTemperaryDirectory() {
            String dirPath = Paths.get("data", "tmp").toString();
            File file = new File(dirPath);
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
