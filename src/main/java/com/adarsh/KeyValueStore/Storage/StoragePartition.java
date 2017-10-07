package com.adarsh.KeyValueStore.Storage;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StoragePartition {

    private static final Logger _LOGGER;
    static {
        _LOGGER = LogManager.getLogger(StoragePartition.class.getName());
    }

    private KeyRange _keyRange;
    private Map<Long, StorageBlob> _primaryStore;
    private VirtualStorageNode _parentNode;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    /**
     * @param parentNode
     * @param keyRange
     */
    public StoragePartition(VirtualStorageNode parentNode, KeyRange keyRange){
        Preconditions.checkNotNull(parentNode,"parentNode is null.");
        Preconditions.checkNotNull(keyRange, "Invalid key range.");
        _keyRange = keyRange;
        _parentNode = parentNode;
        _primaryStore = Collections.synchronizedMap(new HashMap<Long, StorageBlob>());
    }

    /**
     * @return
     */
    public VirtualStorageNode getParentNode() {
        return _parentNode;
    }

    /**
     * @return
     */
    public KeyRange getKeyRange() { return _keyRange; }

    /**
     * @param key
     * @return
     * @throws KeyNotFoundException
     */
    public StorageBlob getValue(long key) throws KeyNotFoundException {
        readLock.lock();
        try {
            if (_primaryStore.containsKey(key))
            {
                _LOGGER.info("Fetching the record for key {}.", key);
                return _primaryStore.get(key);
            } else
            {
                _LOGGER.info("Cannot find the key to fetch value {}.", key);
                throw new KeyNotFoundException();
            }
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * @param key
     * @param data
     * @throws KeyAlreadyExistException
     * @throws KeyOutOfRangeException
     */
    public void insert(long key, StorageBlob data) throws StorageException {
        writeLock.lock();
        try {
            if (_primaryStore.containsKey(key))
            {
                _LOGGER.info("Failed because of the duplicate key {}.", key);
                throw new KeyAlreadyExistException();
            }
            _keyRange.verifyIfKeyIsInRange(key);
            _LOGGER.info("Inserted new key {}", key);
            _primaryStore.put(key, data);
        }
        finally{
            writeLock.unlock();
        }
    }

    /**
     * @param key
     * @throws KeyNotFoundException
     */
    public void delete(long key) throws KeyNotFoundException {
        writeLock.lock();
        try {
            if(_primaryStore.containsKey(key)) {
                _LOGGER.info("Deleting the key {}.", key);
                _primaryStore.remove(key);
            }
            else {
                _LOGGER.info("Cannot find the key to delete {}.", key);
                throw new KeyNotFoundException();
            }
        }
        finally{
            writeLock.unlock();
        }
    }

    /**
     * @param key
     * @param data
     * @throws KeyOutOfRangeException
     */
    public void update(long key, StorageBlob data) throws KeyOutOfRangeException {
        writeLock.lock();
        try {
            _keyRange.verifyIfKeyIsInRange(key);
            StorageBlob currentData = _primaryStore.get(key);
            if(currentData != null){
                if(currentData.getVersion() < data.getVersion()) {
                    _LOGGER.info("Updating the key {} and version {}", key, data.getVersion());
                    _primaryStore.put(key, data);
                }
                else{
                    _LOGGER.warn("Skipping the key update because newer version exist {}", key);
                }
            }
            else {
                _LOGGER.info("No existing key found, treating update as insert for the key {} and version {}", key, data.getVersion());
                _primaryStore.put(key, data);
            }
        }
        finally{
            writeLock.unlock();
        }
    }

    @Override
    public String toString(){
        return "Partition->"+_keyRange.toString();
    }
}

