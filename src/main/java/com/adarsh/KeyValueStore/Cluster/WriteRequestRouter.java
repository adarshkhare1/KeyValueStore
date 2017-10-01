package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyOutOfRangeException;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.adarsh.KeyValueStore.Tasks.WriteOperation;
import com.adarsh.KeyValueStore.Tasks.WriterTaskPool;
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
            throws KeyOutOfRangeException, TimeoutException
    {
        StoragePartition[] partitionsToInsert = _partitionManager.getWritePartitions(key);
        if(partitionsToInsert != null) {
            WriteOperation operation = new WriteOperation();
            operation.setPartitions(partitionsToInsert);
            operation.setKey(key);
            operation.setValue(data);
            operation.setMinimumSuccessfulWrites(_MinimumWriteReplication);
            WriterTaskPool write = new WriterTaskPool(operation);
            write.write();
        }
        else {
            _LOGGER.info("No valid partition found for inserting key {}.", key);
            throw new KeyOutOfRangeException();
        }
    }


}
