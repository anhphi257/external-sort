package com.github.anhphi257.sort;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by phiha on 27/08/2018.
 */
public class Merger implements Runnable {
    private List<File> files;
    private File outFile;

    public Merger(List<File> files, File out) {
        this.files = files;
        this.outFile = out;
    }


    @Override
    public void run() {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile),
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
            writer.close();
            for (File file : files) {
                file.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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