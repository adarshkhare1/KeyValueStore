package com.adarsh.KeyValueStore.Storage;

import com.adarsh.KeyValueStore.Cluster.StorageNode;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class VirtualStorageNode {
    private static final Logger _LOGGER;
    static
    {
        _LOGGER = LogManager.getLogger(StoragePartition.class.getName());
    }

    private StoragePartition _primaryPartition;
    private StorageNode _parentNode;
    private List<StoragePartition> _replications;

    /**
     * @param parentNode
     * @param partitionKeyRange
     */
    public VirtualStorageNode(StorageNode parentNode, KeyRange partitionKeyRange) {
        _primaryPartition = new StoragePartition(this, partitionKeyRange);
        Preconditions.checkNotNull(parentNode, "Parent node is null.");
        _parentNode = parentNode;
    }

    /**
     * @return
     */
    public StorageNode getParentNode() {
        return _parentNode;
    }

    /**
     * @return
     */
    public StoragePartition getPrimaryPartition() {
        return _primaryPartition;
    }
}
