package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyRange;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageMaster extends PhysicalNode {
    private static final String DEFAULT_NODE_NAME = "_master_";
    private static final String DEFAULT_STORAGE_NODE_PREFIX = "_store_";
    private KeyRange _masterKeyRange;
    private List<StorageNode> _nodes;
    private Map<KeyRange, StorageNode> _masterNodeMap;
    private static final Logger _LOGGER;

    static
    {
        _LOGGER = LogManager.getLogger(StorageMaster.class.getName());
    }
    public StorageMaster(NodeEndpoint endpoint) {
        this(DEFAULT_NODE_NAME, endpoint);
    }

    public StorageMaster(String nodeName, NodeEndpoint endpoint) {
        super(nodeName, endpoint);
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
    public void registerNewPhysicalNode(NodeEndpoint storageEndpoint){
        Preconditions.checkState(_nodes.size() == 0, "Only single node storage is supported.");
        Preconditions.checkNotNull(storageEndpoint);
        //In future it willsupport for named nodes
        String storeNodeName = DEFAULT_STORAGE_NODE_PREFIX+_nodes.size();
        //First node, no need to evaluate partitions
        StorageNode storageNode = new StorageNode(storeNodeName, storageEndpoint, _masterKeyRange);
        _LOGGER.info("Registering a new physical storage node named {}", storeNodeName);
        _nodes.add(storageNode);
        _LOGGER.info("{} : Keyrange {} alloczated to storageNode.",
                storeNodeName,
                storageNode.getKeyRange().toString());
        _masterNodeMap.put(storageNode.getKeyRange(), storageNode);
    }

    /**
     * @return an array containing the snapshot list of storage nodes managed by this master node.
     */
    public StorageNode[] getPhysicalStorageNodes(){
        StorageNode[] nodes = _nodes.toArray(new StorageNode[0]);
        return nodes;
    }
}
