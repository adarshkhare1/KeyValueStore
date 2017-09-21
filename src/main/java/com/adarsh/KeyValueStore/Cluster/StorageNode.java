package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyRange;
import com.adarsh.KeyValueStore.Storage.VirtualStorageNode;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageNode extends PhysicalNode {
    private static final int DEFAULT_VIRTUAL_NODE_COUNT = 99;
    private static final Logger _LOGGER;
    static
    {
        _LOGGER = LogManager.getLogger(StorageNode.class.getName());
    }

    private Map<KeyRange, VirtualStorageNode> _virtualStorageNodeMap;
    private KeyRange _keyRange;

    /**
     * @param nodeName
     * @param endpoint
     * @param range
     */
    public StorageNode(String nodeName, NodeEndpoint endpoint, KeyRange range) {
        super(nodeName, endpoint);
        Preconditions.checkNotNull(range);
        _keyRange = range;
        this.initializeVirtualNodes(DEFAULT_VIRTUAL_NODE_COUNT, new KeyRange());
    }

    /**
     * @return
     */
    public KeyRange getKeyRange() {
        return _keyRange;
    }

    /**
     * @return
     */
    public VirtualStorageNode[] getVirtualStorageNodes() {
        VirtualStorageNode[] vNodes =  _virtualStorageNodeMap.values().toArray(new VirtualStorageNode[0]);
        return vNodes;
    }

    private void initializeVirtualNodes(int numberOfVNodes, KeyRange range){
        _virtualStorageNodeMap = new HashMap<>(numberOfVNodes);
        long partitionRangeSpan = range.getKeySpan()/(numberOfVNodes+1);
        long startKey = range.getStartKey();
        for(int i=0;i<numberOfVNodes;i++){
            long endKey;
            if(i < numberOfVNodes-1){
                endKey = startKey+partitionRangeSpan;
            }
            else { //Last partition can be slightly bigger.
                endKey = range.getEndKey();
            }
            KeyRange partitionKeyRange = new KeyRange(startKey, endKey);
            VirtualStorageNode vNode = new VirtualStorageNode(this, partitionKeyRange);
            _virtualStorageNodeMap.put(partitionKeyRange,vNode);
            startKey = partitionKeyRange.getEndKey()+1;
        }
    }
}
