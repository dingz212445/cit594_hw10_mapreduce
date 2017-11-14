package myMapReduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

import javax.swing.JFileChooser;

import mapReduce.MapReduce;
import mapReduce.Pair;

/**
 * @author David Matuszek
 */
public class LetterCounter {

    /**
     * Counts word occurrences.
     * 
     * @param args Unused.
     * @throws FileNotFoundException 
     */
    public static void main(String[] args) throws FileNotFoundException {
    	
    	JFileChooser chooser = new JFileChooser();
    	StringBuilder sb = new StringBuilder();
    	if (chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
    		File file = chooser.getSelectedFile();
    		Scanner input = new Scanner(file);
    		
    		while (input.hasNextLine()) {
    			sb.append(input.nextLine());
    		}
    		input.close();
    	} else {
    		System.out.println("No file selected.");
    	}
    	
        String data = sb.toString();
        System.out.println(data);
        
        List<Pair<String, String>> results;
        results = new MapReduce().execute("Test data", data);
        
        for (Pair<String, String> pair : results) {
            System.out.println(pair.key + "   " + pair.value);
        }
    }
}
