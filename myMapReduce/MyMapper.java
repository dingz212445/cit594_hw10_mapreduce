package myMapReduce;

import mapReduce.Mapper;

public class MyMapper extends Mapper {
    
    @Override
    public void map(String documentID, String document) {
        String[] words = document.split(" ");
        for (String word : words) {
        	if(word.length() == 0) continue;
        	System.out.println("word: " + word);
        	for (int i = 0; i < word.length(); i++) {
        		if(word.charAt(i) <= 'z' && word.charAt(i) >= 'a') {
        			if(i == 0) {
        				emit(word.charAt(i) + "", "11");
        			} else {
        				emit(word.charAt(i) + "", "1");
        			}
        		} else if (word.charAt(i) <= 'Z' && word.charAt(i) >= 'A') {
        			if(i == 0) {
        				emit((word.charAt(i) + 'a' - 'A') + "", "11");
        			} else {
        				emit((word.charAt(i) + 'a' - 'A') + "", "1");
        			}
        		}
        		
        	}
        }
    }
}
