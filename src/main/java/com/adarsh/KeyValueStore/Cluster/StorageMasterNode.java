package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.*;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class StorageMasterNode extends PhysicalNode {
    private static final String DEFAULT_NODE_NAME = "_master_";
    private static final String DEFAULT_STORAGE_NODE_PREFIX = "_store_";
    private static final Logger _LOGGER;

    static
    {
        _LOGGER = LogManager.getLogger(StorageMasterNode.class.getName());
    }

    private KeyRange _masterKeyRange;
    private List<StorageNode> _nodes;
    private final StoragePartitionManager _partitionManager;
    private final WriteRequestRouter _writeRouter;
    private final ReadRequestRouter _readRouter;

    /**
     * @param endpoint
     */
    public StorageMasterNode(NodeEndpoint endpoint) {
        this(DEFAULT_NODE_NAME, endpoint);
    }

    /**
     * @param nodeName
     * @param endpoint
     */
    public StorageMasterNode(String nodeName, NodeEndpoint endpoint) {
        super(nodeName, endpoint);
        _masterKeyRange = new KeyRange(); // Current storage only support full key range.
        _nodes = Collections.synchronizedList(new ArrayList<StorageNode>());
        _partitionManager = new StoragePartitionManager();
        _writeRouter = new WriteRequestRouter(_partitionManager);
        _readRouter = new ReadRequestRouter(_partitionManager);
    }

    /**
     * Create a new node and allocate new load to the node by rebalancing. Node remain inactive.
     * @return
     */
    public static StorageMasterNode createSingleNodeStorage(NodeEndpoint masterEndpoint)
    {
        Preconditions.checkNotNull(masterEndpoint);
        StorageMasterNode master = new StorageMasterNode(masterEndpoint);
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
        _partitionManager.registerStorageNodeForRouting(storageNode);
    }

    /**
     * @return an array containing the snapshot list of storage nodes managed by this master node.
     */
    public StorageNode[] getPhysicalStorageNodes(){
        StorageNode[] nodes = _nodes.toArray(new StorageNode[0]);
        return nodes;
    }

    /**
     * @param key
     * @param data
     * @throws StorageException
     */
    public void insert(long key, StorageBlob data) throws StorageException, TimeoutException
    {
        _writeRouter.insert(key, data);
    }

    /**
     * @param key
     * @throws StorageException
     */
    public StorageBlob getValue(long key) throws StorageException, TimeoutException
    {
        return _readRouter.getValue(key);
    }
}
