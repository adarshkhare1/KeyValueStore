package com.adarsh.KeyValueStore.Tasks;

/**
 * Storage action Read/Insert/Update/Delete
 */
public enum StorageAction {
    /**
     * Delete Key-Value pair from store.
     */
    Delete,
    /**
     * Insert New Key-value pair in store.
     */
    Insert,
    /**
     * Read the the value of key from store.
     */
    Read,
    /**
     * Update the value of given key in store.
     */
    Update
}
