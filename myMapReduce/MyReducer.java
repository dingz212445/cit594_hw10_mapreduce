package myMapReduce;

import java.util.List;

import mapReduce.Reducer;

public class MyReducer extends Reducer {

    @Override
    public void reduce(String word, List<String> counts) {
    	int count = 0;
    	for(String s : counts) {
    		if(s.equals("11")) {
    			count++;
    		}
    	}
        emit(word, count + " " + counts.size());
    }
}

