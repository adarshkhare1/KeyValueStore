package com.adarsh.KeyValueStore.Storage;

import java.util.HashMap;
import java.util.Map;

public class StoragePartition {
    private KeyRange _keyRange;
    private Map<Long, StorageBlob> _primaryStore;

    /**
     *
     */
    public StoragePartition(){
       this(new KeyRange());
    }

    /**
     * @param keyRange
     */
    public StoragePartition(KeyRange keyRange){
        _keyRange = keyRange;
        _primaryStore = new HashMap<>();
    }

    /**
     * @param key
     * @return
     * @throws KeyNotFoundException
     */
    public StorageBlob getValue(long key) throws KeyNotFoundException {
        if(_primaryStore.containsKey(key))
            return _primaryStore.get(key);
        else
            throw new KeyNotFoundException();
    }

    /**
     * @param key
     * @param data
     * @throws KeyAlreadyExistException
     * @throws KeyOutOfRangeException
     */
    public void insert(long key, StorageBlob data) throws StorageException {
        if(_primaryStore.containsKey(key))
            throw new KeyAlreadyExistException();
        _keyRange.verifyIfKeyIsInRange(key);
        _primaryStore.put(key, data);
    }

    /**
     * @param key
     * @throws KeyNotFoundException
     */
    public void delete(long key) throws KeyNotFoundException {
        if(_primaryStore.containsKey(key))
             _primaryStore.remove(key);
        else
            throw new KeyNotFoundException();
    }
}

