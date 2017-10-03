package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;

public class DeleteTask implements Callable<StorageResult> {
    private static final Logger _LOGGER;

    static
    {
        _LOGGER = LogManager.getLogger(DeleteTask.class.getName());
    }



    private final StoragePartition _partition;
    private final long _key;

    public DeleteTask(StoragePartition p, long key){
        _partition = p;
        _key = key;
    }

    @Override
    public StorageResult call() throws Exception
    {
        try {
            _partition.delete(_key);
        }
        catch (StorageException e) {
            _LOGGER.fatal("Failed to insert key {}.", _key);
            _LOGGER.fatal(ExceptionUtils.getStackTrace(e));
            return StorageResult.Failure;
        }
        return StorageResult.Success;
    }

    /**
     * @return
     */
    public StoragePartition getPartition()
    {
        return _partition;
    }

    /**
     * @return
     */
    public long getKey()
    {
        return _key;
    }

}
