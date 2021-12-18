package de.tum.i13.server.persistentstorage.btree.chunk;

import java.io.Serializable;

/**
 * Data class representing a {@link Serializable} key-value pair. Keys are
 * Strings and value types are configured through the generic {@code V}
 * 
 * @param <V> Type of vlaue
 */
public class Pair<V> implements Serializable {

    private static final long serialVersionUID = 6529685098267755681L;

    /**
     * Pair key
     */
    public final String key;

    /**
     * Pair value
     */
    public final V value;

    /**
     * Create new pair
     * 
     * @param key   key
     * @param value value
     */
    public Pair(String key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
 
        if (!(obj instanceof Pair)) {
            return false;
        }
         
        // typecast o to Complex so that we can compare data members
        Pair<?> c = (Pair<?>) obj;
         
        // Compare the data members and return accordingly
        return  c.key.equals(this.key) && c.value.equals(this.value);
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return this.key + " | " + this.value;
    }
}
