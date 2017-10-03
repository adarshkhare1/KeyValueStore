package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyOutOfRangeException;
import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.adarsh.KeyValueStore.Tasks.DeleteOperation;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeoutException;

public class DeleteRequestRouter {
    private static final Logger _LOGGER;

    private static final int _MinimumWriteReplication = 1;

    static
    {
        _LOGGER = LogManager.getLogger(DeleteRequestRouter.class.getName());
    }

    private final StoragePartitionManager _partitionManager;


    /**
     *
     */
    public  DeleteRequestRouter(StoragePartitionManager partitionManager){
        Preconditions.checkNotNull(partitionManager, "partitionManager is null.");
        _partitionManager = partitionManager;

    }

    /**
     * @param key
     * @throws KeyOutOfRangeException
     * @throws TimeoutException
     */;
    public void delete(long key)
            throws StorageException, TimeoutException
    {
        StoragePartition[] partitionsToInsert = _partitionManager.getWritePartitions(key);
        if(partitionsToInsert != null) {
            DeleteOperation operation = new DeleteOperation();
            operation.setPartitions(partitionsToInsert);
            operation.setKey(key);
            operation.execute();
        }
        else {
            _LOGGER.info("No valid partition found for inserting key {}.", key);
            throw new KeyOutOfRangeException();
        }
    }

}
