package com.adarsh.KeyValueStore;

import java.util.List;

public class VirtualStorageNode {
    private StoragePartition _masterPartition;
    private List<StoragePartition> _replications;

    public VirtualStorageNode(KeyRange partitionKeyRange) {
        _masterPartition = new StoragePartition(partitionKeyRange);
    }
}
