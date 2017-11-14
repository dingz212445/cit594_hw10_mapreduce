package mapReduce;

import java.util.List;

/**
 * A superclass to be extended by the user's MyReducer class.
 * @author David Matuszek
 */
public abstract class Reducer extends Thread {
    private String key;
    private List<String> listOfValues;
    
    /** Results produced by the <code>reduce</code> method.*/
    private List<Pair<String, String>> finalResults;

    /**
     * Sets the values to be given to the <code>reduce</code> method,
     * and saves a link to a shared storage area for the results.
     * 
     * @param key The key to be given to the <code>reduce</code>
     *  method.
     * @param listOfValues The values to be given to the
     *  <code>reduce</code> method.
     * @param finalResults A reference to the shared area in which
     *  to put results.
     */
    public void initialize(String key, List<String> listOfValues,
                           List<Pair<String, String>> finalResults) {
        this.key = key;
        this.listOfValues = listOfValues;
        this.finalResults = finalResults;
    }
    
    /**
     * Executes the user's <code>reduce</code> method in a new thread.
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        reduce(key, listOfValues);
    }
    
    /**
     * Given a single key and a list of values for that key,
     * reduces the values to a single value, and emits the
     * resultant key-value pair. The nature of the reduction
     * depends on the type of work to be done.
     * 
     * @param key A specific key, to be retained and emitted.
     * @param values The values to be reduced to a single value.
     */
    public abstract void reduce(String key, List<String> values);
    
    /**
     * Called by the user's <code>reduce</code> method to accumulate
     *  results.
     * @param key A key found by the <code>reduce</code> method.
     * @param value The corresponding value found by the
     *  <code>reduce</code> method.
     */
    public final void emit(String key, String value) {
        synchronized (finalResults) {
            finalResults.add(new Pair<String, String>(key, value));
        }
    }
}

