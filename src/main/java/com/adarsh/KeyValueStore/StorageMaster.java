package com.adarsh.KeyValueStore;

import com.google.common.base.Preconditions;

import java.util.*;

public class StorageMaster extends PhysicalNode{
    KeyRange _masterKeyRange;
    List<StorageNode> _nodes;
    Map<KeyRange, StorageNode> _masterNodeMap;

    public StorageMaster(NodeEndpoint endpoint) {
        super(endpoint);
        _masterKeyRange = new KeyRange(); // Current storage only support full key range.
        _nodes = Collections.synchronizedList(new ArrayList<StorageNode>());
        _masterNodeMap = Collections.synchronizedMap(new HashMap<KeyRange, StorageNode>());
    }


    /**
     * Create a new node and allocate new load to the node by rebalancing. Node remain inactive.
     * @return
     */
    public static StorageMaster createSingleNodeStorage(NodeEndpoint masterEndpoint)
    {
        Preconditions.checkNotNull(masterEndpoint);
        StorageMaster master = new StorageMaster(masterEndpoint);
        return master;
    }

    /**
     * Add a physical node in the storage. Currently only support single node storage.
     * @param storageEndpoint
     */
    public void addNewPhysicalNode(NodeEndpoint storageEndpoint){
        Preconditions.checkState(_nodes.size() == 0, "Only single node storage is supported.");
        Preconditions.checkNotNull(storageEndpoint);
        StorageNode storageNode = new StorageNode(storageEndpoint, _masterKeyRange); //First node, no need to evaluate partitions
        _nodes.add(storageNode);
        _masterNodeMap.put(storageNode.getKeyRange(), storageNode);
    }
}
