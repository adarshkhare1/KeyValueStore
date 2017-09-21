package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.*;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class WriteRequestRouter {
    private static final Logger _LOGGER;

    static
    {
        _LOGGER = LogManager.getLogger(WriteRequestRouter.class.getName());
    }

    private final StoragePartitionManager _partitionManager;

    /**
     *
     */
    public  WriteRequestRouter(StoragePartitionManager partitionManager){
        Preconditions.checkNotNull(partitionManager, "partitionManager is null.");
        _partitionManager = partitionManager;
    }


    /**
     * @param key
     * @param data
     * @throws StorageException
     */
    public void insert(long key, StorageBlob data) throws StorageException {
        StoragePartition[] partitionsToInsert = _partitionManager.getWritePartitions(key);
        if(partitionsToInsert != null) {
            for(StoragePartition p:partitionsToInsert) {
                p.insert(key, data);
                _LOGGER.info("Inserting key {} to partition {}.", key, p);
            }
        }
        else {
            _LOGGER.info("No valid partion found for inserting key {}.", key);
            throw new KeyOutOfRangeException();
        }
    }
}
