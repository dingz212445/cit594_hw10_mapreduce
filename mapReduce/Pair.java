package mapReduce;

/**
 * A single key-value pair. For simplicity, the <code>key</code>
 * and <code>value</code> fields are public, so no getters or
 * setters are necessary.
 * 
 * @author David Matuszek
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class Pair<K, V> {
    
    /** The first element in this Pair. */
    public K key;
    /** The second element in this Pair. */
    public V value;
    
    /**
     * Constructor.
     * @param key The key.
     * @param value The value.
     */
    Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }
}
