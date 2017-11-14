package mapReduce;

/**
 * Partitions a string into approximately equal-sized "chunks,"
 * broken at spaces.
 * 
 * @author David Matuszek
 */
public class Splitter {
    
    /**
     * Returns a String of length approximately data.length()/howMany.
     * Each String, except possibly the first, will begin with a space.
     * 
     * @param data The String to split into approximately equal pieces.
     * @param i Which piece is wanted (0 .. howMany - 1).
     * @param howMany How many pieces are wanted.
     * @return The i-th portion of the input String.
     */
    public static String split(String data, int i, int howMany) {
        int first = getIndex(data, i, howMany);
        int last = getIndex(data, i + 1, howMany);
        return data.substring(first, last);
    }

    /**
     * Finds the start index of a substring of the data, such that
     * either (1) the substring starts at the beginning of the data,
     * or (2) the substring begins with a space character and is
     * near the beginning of the i-th of howMany approximately
     * equal substrings of the data. All substrings thus found will
     * partition the data; that is, they will be nonoverlapping and
     * exhaustive.
     * 
     * @param data The string to be partitioned.
     * @param i The number (0 .. howMany-1) of the desired partition.
     * @param howMany How many partitions to use.
     * @return The index of the beginning of the i-th partition.
     */
    private static int getIndex(String data, int i, int howMany) {
        if (i == 0) {
            return 0;
        }
        else if (i == howMany) {
            return data.length();
        } else {
            return data.indexOf(" ", (i * (data.length()) / howMany)); 
        }
    }
}

