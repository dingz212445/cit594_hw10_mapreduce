package mapReduce;

import java.util.LinkedList;
import java.util.List;

/**
 * A superclass to be extended by the user's MyMapper class.
 * @author David Matuszek
 */
public abstract class Mapper extends Thread {
    private String document;
    private String data;
    
    /** Results produced by the mapper. */
    protected List<Pair<String, String>> rawResults;
    
    /**
     * Sets the values to be given to the <code>map</code> method, and creates
     * a storage area for the results.
     * 
     * @param dataSource The key to be given to the <code>map</code> method.
     * @param longString The value to be given to the <code>map</code> method.
     */
    public final void initialize(String dataSource, String longString) {
        document = dataSource;
        data = longString;
        rawResults = new LinkedList<Pair<String, String>>();
    }
    
    /**
     * Executes the user's <code>map</code> method in a new Thread.
     * @see java.lang.Thread#run()
     */
    @Override
    public final void run() {
        map(document, data);
    }
    
    /**
     * Given a source document and the contents of that document,
     * extracts and emits a number of key/value pairs. The particular
     * keys and values depend on the type of work to be done.
     * 
     * @param document The source of the data.
     * @param value The data to be examined and mapped to key-value pairs.
     */
    public abstract void map(String document, String value);
    
    /**
     * Called by the user's <code>map</code> method to accumulate results.
     * @param key A key found by the <code>map</code> method.
     * @param value The corresponding value found by the <code>map</code> method.
     */
    public final void emit(String key, String value) {
        rawResults.add(new Pair<String, String>(key, value));
    }
}

