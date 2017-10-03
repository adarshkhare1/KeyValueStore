package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyOutOfRangeException;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.adarsh.KeyValueStore.Tasks.ReadOperation;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class ReadRequestRouter {
    private static final Logger _LOGGER;
    private static final ExecutorService _Reader;
    private static final int _MinimumSuccessfulReads = 1;

    static
    {
        _LOGGER = LogManager.getLogger(ReadRequestRouter.class.getName());
        _Reader = Executors.newFixedThreadPool(10);
    }

    private final StoragePartitionManager _partitionManager;

    /**
     *
     */
    public  ReadRequestRouter(StoragePartitionManager partitionManager){
        Preconditions.checkNotNull(partitionManager, "partitionManager is null.");
        _partitionManager = partitionManager;
    }

    /**
     * @param key
     * @throws StorageException
     * @throws InterruptedException
     */
    public StorageBlob getValue(long key)
            throws StorageException, TimeoutException
    {
        StoragePartition[] partitionsToRead = _partitionManager.getReadPartitions(key);
        StorageBlob value = null;
        if(partitionsToRead != null) {
            ReadOperation operation = new ReadOperation();
            operation.setPartitions(partitionsToRead);
            operation.setKey(key);
            operation.setMinimumSuccessfulReads(_MinimumSuccessfulReads);
            value = operation.execute();
        }
        else {
            _LOGGER.info("No valid partion found to fetch key {}.", key);
            throw new KeyOutOfRangeException();
        }
        return value;
    }
}
