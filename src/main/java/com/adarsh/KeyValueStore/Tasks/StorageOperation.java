package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.google.common.base.Preconditions;

import java.util.concurrent.TimeoutException;

public abstract class StorageOperation<T> {
    private StoragePartition[] _partitions;
    private long _key;

    public abstract StorageAction getStorageAction();
    public abstract T execute() throws TimeoutException, StorageException;

    public void setPartitions(StoragePartition[] partitions) {
        Preconditions.checkNotNull(partitions);
        this._partitions = partitions;
    }
    public StoragePartition[] getPartitions()
    {
        return _partitions;
    }

    public long getKey() {
        return _key;
    }

    public void setKey(long key) { this._key = _key; }
}
