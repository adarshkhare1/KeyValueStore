package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyRange;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.adarsh.KeyValueStore.Storage.VirtualStorageNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class StoragePartitionManager {
    private static final Logger _LOGGER;

    static
    {
        _LOGGER = LogManager.getLogger(StoragePartitionManager.class.getName());
    }

    private final Map<KeyRange, StoragePartition> _keyRangeStoragePartitionMap;
    private final TreeSet<StoragePartition> _partitionSet;

    /**
     *
     */
    public StoragePartitionManager(){
        _keyRangeStoragePartitionMap = Collections.synchronizedMap(new HashMap<KeyRange, StoragePartition>());
        _partitionSet = new TreeSet<StoragePartition>(new Comparator<StoragePartition>() {
            @Override
            public int compare(StoragePartition p1, StoragePartition p2) {
                long key1 = p1.getKeyRange().getStartKey();
                long key2 = p2.getKeyRange().getStartKey();
                if(key2 > key1) {return -1;}
                if(key2 < key1) {return 1;}
                return 0;
            }
        });
    }
    /**
     * @param storageNode
     */
    public void registerStorageNodeForRouting(StorageNode storageNode){
        _LOGGER.info("registering new storage node {} in storage.", storageNode);
        for(VirtualStorageNode node:storageNode.getVirtualStorageNodes()){
            _keyRangeStoragePartitionMap.put(storageNode.getKeyRange(), node.getPrimaryPartition());
            _partitionSet.add(node.getPrimaryPartition());
        }
    }
    /**
     * @param key
     * @return
     */
    public StoragePartition[] getWritePartitions(long key){
        return new StoragePartition[]{findPartitionToInsert(key)};
    }

    /**
     * @param key
     * @return
     */
    public StoragePartition[] getReadPartitions(long key){
        return new StoragePartition[]{findPartitionToInsert(key)};
    }

    /**
     * Iterate through partitions and find the partition to insert key.
     * @param key
     * @return partition where key can be inserted, return null if no valid partition found to insert key.
     */
    private StoragePartition findPartitionToInsert(long key) {
        StoragePartition partitionToInsert = null;
        Iterator<StoragePartition> iterator = _partitionSet.iterator();
        while (iterator.hasNext()){
            StoragePartition p = iterator.next();
            if(p.getKeyRange().getStartKey() > key) {
                break;
            }
            partitionToInsert = p;
            _LOGGER.info("Iterating through partition {}.", p);
        }
        return partitionToInsert;
    }
}
