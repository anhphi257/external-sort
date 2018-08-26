package com.github.anhphi257.old;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class ExternalSort {

    private static int NUM_THREAD = 4;


    public static List<File> splitAndSort(File inputFile, long blockSize, String tmpDir, Comparator<String> comparator, Charset cs) throws IOException {
        List<File> files = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), cs));
        String line = "";
        File dir = new File(tmpDir);
        for (File file : dir.listFiles()) {
            file.delete();
        }

        List<String> lines = new ArrayList<>();
        long currentSize = 0;
        int index = 0;
        while ((line = reader.readLine()) != null) {
            int length = line.length() * 2; //Unicode 2 bytes per character
            if (length + currentSize < blockSize) {
                lines.add(line);
                currentSize += length;
            } else {
                String fileName = Paths.get(tmpDir, inputFile.getName() + "-" + index).toString();
                File f = new File(fileName);
                files.add(f);
                sortAndSave(lines, f, comparator, cs);
                index++;
                lines.clear();
                lines.add(line);
                currentSize = 0;

            }
        }
        String fileName = Paths.get(tmpDir, inputFile.getName() + "-" + index).toString();
        if (lines.size() > 0) {
            File f = new File(fileName);
            files.add(f);
            sortAndSave(lines, f, comparator, cs);
        }
        return files;
    }

    public static void sortAndSave(List<String> lines, File file, Comparator<String> comparator, Charset cs) throws
            IOException {

        System.out.println("saving: " + file.getName());
        lines = lines.parallelStream().sorted(comparator).collect(Collectors.toList());
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false), cs))) {
            lines.forEach(line -> {
                try {
                    if (line.length() > 0)
                        writer.write(line + "\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

    }

    public static void mergeSortedFiles(List<File> files, File outFile) throws IOException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile),
                Charset.forName("UTF-8")));
        PriorityQueue<FileCache> queue = new PriorityQueue<>(Comparator.comparing(FileCache::peek));
        for (File file : files) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
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
        for (File file : files) {
            file.delete();
        }
    }

    private static long freeMemory() {
        System.gc();
        Runtime r = Runtime.getRuntime();
        long usedMem = r.totalMemory() - r.freeMemory();
        long freeMem = r.maxMemory() - usedMem;
        return freeMem;
    }

    private static long blockSize() {
        long freeMem = freeMemory();
        return (long) (freeMem * 0.75);
    }

    private static void mkdir(String dir) {
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    public static void main(String[] args) throws IOException {
        Comparator<String> comparator = Comparator.naturalOrder();
        String tmpDir = "data/tmp";
        splitAndSort(new File("data/100MB"), 100l * 1024, tmpDir, comparator, Charset.forName("UTF-8"));
    }


}

final class FileCache {
    private BufferedReader reader;
    private String cache;

    public FileCache(BufferedReader reader) throws IOException {
        this.reader = reader;
        this.cache = reader.readLine();
    }

    public String peek() {
        return this.cache;
    }

    public String poll() throws IOException {
        String answer = this.cache;
        this.cache = reader.readLine();
        return answer;
    }

    public boolean empty() {
        return this.cache == null;
    }
}