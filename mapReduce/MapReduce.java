package mapReduce;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A baby version of Google's MapReduce framework. This version uses
 * Threads on a single computer rather than processes distributed
 * across a network, but is otherwise as similar as it could readily
 * be made.
 * 
 * To use this framework, supply a package named "myMapReduce" containing: 
 *    <ul><li>A <code>MyMapper</code> class containing a
 *            <code>map</code> function,</li> 
 *        <li>A <code>Reducer</code> class containing a
 *            <code>reduce</code> function, and</li> 
 *        <li>A "main" class which creates a <code>MapReduce</code>
 *            object, calls this object's <code>execute</code>
 *            method, and does something with the results.</li>
 *    </ul>
 * @author David Matuszek
 */
public class MapReduce {
    
    /** Number of Threads to use for mappers.*/
    private static final int NUMBER_OF_MAPPER_THREADS = 8;
    
    /** Number of Threads to use for reducers.*/
    private static final int NUMBER_OF_REDUCER_THREADS = 4;
    
    /** Results produced by the combiner.*/
    protected Map<String, List<String>> combinedResults;
    
    /** Results produced by the reducer.*/
    protected List<Pair<String, String>> finalResults;
    
    /**
     * Executes the MapReduce algorithm on the given data. The user must
     * supply a package named "myMapReduce" containing:
     * <ul><li>A MyMapper class containing a map function,</li>
     *     <li>A Reducer class containing a reduce function,</li> and
     *     <li>A main class which calls this class's execute method.</li>
     * </ul>
     * @param document An identifier for the document being processed.
     * @param data The data to process.
     * @return A list of key/value pairs.
     */
    public List<Pair<String, String>> execute(String document, String data) {
        combinedResults = new ConcurrentHashMap<String, List<String>>();
        finalResults = new LinkedList<Pair<String, String>>();
        Mapper[] mappers = new Mapper[NUMBER_OF_MAPPER_THREADS];
        
        // map: (document, value) -> list of key-value pairs
        for (int i = 0; i < mappers.length; i++) {
            mappers[i] = new myMapReduce.MyMapper();
            String chunk = Splitter.split(data, i, mappers.length);
            mappers[i].initialize(document, chunk);
            mappers[i].start();
        }
        joinAll(mappers);
        // combine: list of key-value pairs -> map of keys to list of values
        for (int i = 0; i < mappers.length; i++) {
            combine(mappers[i].rawResults);
        }
        // reduce: map of keys to list of values -> list of
        //                        key - reduced value pairs
        ExecutorService exec =
            Executors.newFixedThreadPool(NUMBER_OF_REDUCER_THREADS);
        for (String key : combinedResults.keySet()) {
            Reducer reducer = new myMapReduce.MyReducer();
            reducer.initialize(key, combinedResults.get(key), finalResults);
            exec.execute(reducer);
        }
        exec.shutdown();
        try {
            exec.awaitTermination(1, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return finalResults;
    }

    /**
     * Wait for all the given threads to finish.
     * @param threads The threads to join.
     */
    private void joinAll(Thread[] threads) {
        for (int i = 0; i < NUMBER_OF_MAPPER_THREADS; i++) {
            try {
                threads[i].join();
            }
            catch (InterruptedException e) {
                System.out.println("joinAll(threads) has been interrupted.");
                System.exit(1);
            }
        }
    }

    /**
     * Combines the list <code>rawResults</code> of key-value pairs
     * produced by the mappers (where the same key may occur many
     * times) into the map <code>combinedResults</code> which
     * groups all values according to their associated key.
     * 
     * @param rawResults Data produced by the mappers, in the form
     *        <code>List&lt;Pair&lt;String, String&gt;&gt;</code>.
     */
    public void combine(List<Pair<String, String>> rawResults) {
        List<Pair<String, String>> typedRawResults = rawResults;
        for (Pair<String, String> pair : typedRawResults) {
            String key = pair.key;
            String value = pair.value;
            List<String> list = combinedResults.get(key);
            if (list == null) {
                list = new LinkedList<String>();
                combinedResults.put(key, list);
            }
            list.add(value);
        }
    }
}
