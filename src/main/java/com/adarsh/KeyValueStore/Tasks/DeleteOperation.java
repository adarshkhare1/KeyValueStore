package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.ReadConsistencyException;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DeleteOperation extends StorageOperation<StorageResult> {
    private static final Logger _LOGGER;
    private static final int _DeleteTimeout = 10000; //milliseconds

    static
    {
        _LOGGER = LogManager.getLogger(DeleteOperation.class.getName());
    }

    @Override
    public StorageAction getStorageAction() {
        return StorageAction.Delete;
    }

    @Override
    public StorageResult execute() throws TimeoutException, StorageException {
        ExecutorCompletionService<StorageResult> executionPool
                = StorageTaskPool.CreateNewTaskCompletionService(this);
        List<Future<StorageResult>> futures = new ArrayList<>();
        for(StoragePartition p:super.getPartitions()) {
            DeleteTask task = new DeleteTask(p, super.getKey());
            futures.add(executionPool.submit(task));
            _LOGGER.info("Submitted DeleteTask to delete key {} to partition {}.", super.getKey(), p);
        }
        return getResult(executionPool, super.getPartitions().length);
    }

    /**
     * Wait for minimum number of matching reads to return
     * @return
     * @throws TimeoutException
     * @throws ReadConsistencyException
     */
    private StorageResult getResult(ExecutorCompletionService<StorageResult> executionPool,
                                    int numberOfDeletes) throws TimeoutException, ReadConsistencyException
    {
        long tickCount = currentTimeMillis();
        long timeout = _DeleteTimeout;
        StorageResult result = StorageResult.Success;
        int resultCount = 0;
        Map<StorageBlob, Integer> readCountMap = new HashMap<>();
        try {
            while(resultCount < numberOfDeletes && timeout > 0) {
                Future<StorageResult> resultFuture = executionPool.poll(timeout, MILLISECONDS);
                try {
                    StorageResult s = resultFuture.get();
                    if (s != null) { resultCount++; }
                    if(s != StorageResult.Success) result = StorageResult.Failure;
                    long newTickCount = currentTimeMillis();
                    timeout = timeout - (newTickCount - tickCount);
                    tickCount = newTickCount;
                }
                catch (ExecutionException e) {
                    _LOGGER.warn(e.getMessage());
                    throw new TimeoutException();
                }
            }
        }
        catch (InterruptedException e) {
            _LOGGER.warn(e.getMessage());
            throw new TimeoutException();
        }
        return result;
    }
}
