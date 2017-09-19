package com.adarsh.KeyValueStore;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageNode extends PhysicalNode{
    private static final int DEFAULT_VIRTUAL_NODE_COUNT = 99;
    private Map<KeyRange, VirtualStorageNode> storageNodes;
    KeyRange _keyRange;

    public StorageNode(NodeEndpoint endpoint, KeyRange range) {
        super(endpoint);
        Preconditions.checkNotNull(range);
        _keyRange = range;
        this.initializeVirtualNodes(DEFAULT_VIRTUAL_NODE_COUNT, new KeyRange());
    }
    public KeyRange getKeyRange() {
        return _keyRange;
    }

    private void initializeVirtualNodes(int numberOfVNodes, KeyRange range){
        storageNodes = new HashMap<>(numberOfVNodes);
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
            VirtualStorageNode vNode = new VirtualStorageNode(partitionKeyRange);
            storageNodes.put(partitionKeyRange,vNode);
            startKey = partitionKeyRange.getEndKey()+1;
        }
    }
}
