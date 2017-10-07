package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyOutOfRangeException;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.adarsh.KeyValueStore.Tasks.InsertOperation;
import com.adarsh.KeyValueStore.Tasks.UpdateOperation;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeoutException;

public class WriteRequestRouter {
    private static final Logger _LOGGER;

    private static final int _MinimumWriteReplication = 1;

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
     * @throws KeyOutOfRangeException
     * @throws TimeoutException
     */
    public void insert(long key,
                       StorageBlob data)
            throws StorageException, TimeoutException
    {
        StoragePartition[] partitionsToInsert = _partitionManager.getWritePartitions(key);
        if(partitionsToInsert != null) {
            InsertOperation operation = new InsertOperation();
            operation.setPartitions(partitionsToInsert);
            operation.setKey(key);
            operation.setValue(data);
            operation.setMinimumSuccessfulWrites(_MinimumWriteReplication);
            operation.execute();
        }
        else {
            _LOGGER.info("No valid partition found for inserting key {}.", key);
            throw new KeyOutOfRangeException();
        }
    }

    /**
     * @param key
     * @param data
     * @throws KeyOutOfRangeException
     * @throws TimeoutException
     */
    public void update(long key,
                       StorageBlob data)
            throws StorageException, TimeoutException
    {
        StoragePartition[] partitionsToUpdate = _partitionManager.getWritePartitions(key);
        if(partitionsToUpdate != null) {
            UpdateOperation operation = new UpdateOperation();
            operation.setPartitions(partitionsToUpdate);
            operation.setKey(key);
            operation.setValue(data);
            operation.setMinimumSuccessfulUpdates(_MinimumWriteReplication);
            operation.execute();
        }
        else {
            _LOGGER.info("No valid partition found for inserting key {}.", key);
            throw new KeyOutOfRangeException();
        }
    }


}
