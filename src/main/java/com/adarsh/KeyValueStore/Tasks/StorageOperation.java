package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class StorageOperation<T> {
    private StoragePartition[] _partitions;
    private long _key;

    public abstract StorageAction getStorageAction();
    public abstract T execute() throws TimeoutException, StorageException;

    public void setPartitions(StoragePartition[] partitions) {
        Preconditions.checkNotNull(partitions);
        this._partitions = partitions;
    }
    public StoragePartition[] getPartitions()
    {
        return _partitions;
    }

    public long getKey() {
        return _key;
    }

    public void setKey(long key) { this._key = _key; }

    protected void waitForMinimumWrites(ExecutorCompletionService<StorageResult> executionPool,
                                        int minWrites,
                                        long timeout,
                                        Logger logger) throws TimeoutException
    {
        long tickCount = currentTimeMillis();
        int successCount = 0;
        try
        {
            while (successCount < minWrites && timeout > 0)
            {
                Future<StorageResult> resultFuture = null;
                resultFuture = executionPool.poll(timeout, MILLISECONDS);

                try
                {
                    if (resultFuture.get() == StorageResult.Success) successCount++;
                    long newTickCount = currentTimeMillis();
                    timeout = timeout - (newTickCount - tickCount);
                    tickCount = newTickCount;
                } catch (ExecutionException e)
                {
                    logger.warn(e.getMessage());
                    throw new TimeoutException();
                }
            }
        } catch (InterruptedException e)
        {
            logger.warn(e.getMessage());
            throw new TimeoutException();
        }
    }
}
