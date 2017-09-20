package com.adarsh.KeyValueStore.Storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StoragePartition {
    private KeyRange _keyRange;
    private Map<Long, StorageBlob> _primaryStore;
    private final Logger _LOGGER;

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
        _primaryStore = Collections.synchronizedMap(new HashMap<Long, StorageBlob>());
        _LOGGER = LogManager.getLogger(StoragePartition.class.getName());
    }

    /**
     * @param key
     * @return
     * @throws KeyNotFoundException
     */
    public StorageBlob getValue(long key) throws KeyNotFoundException {
        if(_primaryStore.containsKey(key)) {
            _LOGGER.info("Fetching the record for key {}.", key);
            return _primaryStore.get(key);
        }
        else {
            _LOGGER.info("Cannot find the key to fetch value {}.", key);
            throw new KeyNotFoundException();
        }
    }

    /**
     * @param key
     * @param data
     * @throws KeyAlreadyExistException
     * @throws KeyOutOfRangeException
     */
    public void insert(long key, StorageBlob data) throws StorageException {
        if(_primaryStore.containsKey(key)) {
            _LOGGER.info("Failed because of the duplicate key {}.", key);
            throw new KeyAlreadyExistException();
        }
        _keyRange.verifyIfKeyIsInRange(key);
        _LOGGER.info("Inserted new key %d.", key);
        _primaryStore.put(key, data);
    }

    /**
     * @param key
     * @throws KeyNotFoundException
     */
    public void delete(long key) throws KeyNotFoundException {
        if(_primaryStore.containsKey(key)) {
            _LOGGER.info("Deleting the key {}.", key);
            _primaryStore.remove(key);
        }
        else {
            _LOGGER.info("Cannot find the key to delete {}.", key);
            throw new KeyNotFoundException();
        }
    }
}

