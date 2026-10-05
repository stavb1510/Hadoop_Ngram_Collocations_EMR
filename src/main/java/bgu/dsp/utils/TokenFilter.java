package bgu.dsp.utils;

public class TokenFilter {

    public static boolean isValidWord(String s) {
        if (s == null) return false;

        s = s.trim();
        if (s.length() < 3) return false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            // English letters
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) continue;

            // Hebrew letters
            if (c >= '\u0590' && c <= '\u05FF') continue;

            return false;
        }
        return true;
    }
}