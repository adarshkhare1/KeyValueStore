package com.adarsh.KeyValueStore.Cluster;

import com.adarsh.KeyValueStore.Storage.KeyOutOfRangeException;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReadRequestRouter {
    private static final Logger _LOGGER;

    static
    {
        _LOGGER = LogManager.getLogger(ReadRequestRouter.class.getName());
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
     */
    public StorageBlob getValue(long key) throws StorageException {
        StoragePartition[] partitionsToRead = _partitionManager.getReadPartitions(key);
        StorageBlob value = null;
        if(partitionsToRead != null) {
            for(StoragePartition p:partitionsToRead) {
                value = p.getValue(key);
                _LOGGER.info("Fetching key {} fromo partition {}.", key, p);
            }
        }
        else {
            _LOGGER.info("No valid partion found to fetch key {}.", key);
            throw new KeyOutOfRangeException();
        }
        return value;
    }
}
