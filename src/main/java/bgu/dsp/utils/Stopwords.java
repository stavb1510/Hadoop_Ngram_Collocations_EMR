package bgu.dsp.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Stopwords {
    
    private Set<String> stopWords;

    public Stopwords() {
        this.stopWords = new HashSet<>();
        loadStopWords("heb-stopwords.txt");
        loadStopWords("eng-stopwords.txt");
    }

    private void loadStopWords(String fileName) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                System.err.println("Error: Could not find " + fileName);
                return;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isStopWord(String word) {
        return stopWords.contains(word);
    }
}