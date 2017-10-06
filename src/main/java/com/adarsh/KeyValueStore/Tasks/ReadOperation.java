package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.*;
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

public class ReadOperation extends StorageOperation<StorageBlob> {
    private static final Logger _LOGGER;
    private static final int _ReadTimeout = 10000; //milliseconds


    static
    {
        _LOGGER = LogManager.getLogger(ReadOperation.class.getName());
    }

    private int _minimumSuccessfulReads = 1;

    public int getMinimumSuccessfulReads() {
        return _minimumSuccessfulReads;
    }

    public void setMinimumSuccessfulReads(int _minimumSuccessfulReads) {
        this._minimumSuccessfulReads = _minimumSuccessfulReads;
    }

    @Override
    public StorageAction getStorageAction() {
        return StorageAction.Read;
    }

    @Override
    public StorageBlob execute() throws TimeoutException, StorageException {
        ExecutorCompletionService<StorageBlob> executionPool
                = StorageTaskPool.CreateNewTaskCompletionService(this);
        List<Future<StorageBlob>> futures = new ArrayList<>();
        for(StoragePartition p:super.getPartitions()) {
            ReadTask task = new ReadTask(p, super.getKey());
            futures.add(executionPool.submit(task));
            _LOGGER.info("Submitted ReadTask to fetch value for  key {} to partition {}.", super.getKey(), p);
        }
        return GetValue(executionPool, _minimumSuccessfulReads);
    }

    /**
     * Wait for minimum number of matching reads to return
     * @param minimumMatchReads
     * @return
     * @throws TimeoutException
     * @throws ReadConsistencyException
     */
    private StorageBlob GetValue(ExecutorCompletionService<StorageBlob> executionPool,
                                 int minimumMatchReads) throws TimeoutException, ReadConsistencyException, KeyNotFoundException {
        long tickCount = currentTimeMillis();
        long timeout = _ReadTimeout;
        StorageBlob result = null;
        int resultCount = 0;
        Map<StorageBlob, Integer> readCountMap = new HashMap<>();
        try {
            while(resultCount < minimumMatchReads && timeout > 0) {
                Future<StorageBlob> resultFuture = executionPool.poll(timeout, MILLISECONDS);
                try {
                    StorageBlob s = resultFuture.get();
                    if (s != null) {
                        int count = updateReadCountMap(readCountMap, s);
                        if (count > resultCount) {
                            result = s;
                            resultCount = count + 1;
                        }
                    }
                    else {
                        if(resultCount > 0)
                            throw new ReadConsistencyException();
                        else
                            throw new KeyNotFoundException();
                    }
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

    /**
     * Update the readcount map and return the count for the given storage blob.
     * @param readCountMap
     * @param s
     * @return
     */
    private int updateReadCountMap(Map<StorageBlob, Integer> readCountMap, StorageBlob s)
    {
        int count = readCountMap.containsKey(s)? readCountMap.get(s)+1 : 1;
        readCountMap.put(s, count);
        return count;
    }
}
