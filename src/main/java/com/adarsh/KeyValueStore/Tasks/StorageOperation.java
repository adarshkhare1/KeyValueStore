package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.google.common.base.Preconditions;

public abstract class StorageOperation {
    private StoragePartition[] _partitions;
    private long _key;


    public StoragePartition[] getPartitions()
    {
        return _partitions;
    }

    public void setPartitions(StoragePartition[] partitions) {
        Preconditions.checkNotNull(partitions);
        this._partitions = partitions;
    }

    public long getKey() {
        return _key;
    }

    public void setKey(long key) {
        this._key = _key;
    }
}
