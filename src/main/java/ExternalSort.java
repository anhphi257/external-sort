import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExternalSort {


    public static List<File> sortInBatch(File inputFile, long maxSize, String tmpDir, Comparator<String> comparator, Charset cs) throws IOException {
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
            int length = line.length();
            if (length + currentSize < maxSize) {
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

        private static void sortAndSave (List < String > lines, File file, Comparator < String > comparator, Charset cs) throws
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


        public static void main (String[]args) throws IOException {
            Comparator<String> comparator = Comparator.naturalOrder();
            String tmpDir = "data/tmp";
            sortInBatch(new File("data/100MB"), 100l * 1024, tmpDir, comparator, Charset.forName("UTF-8"));
        }
    }
