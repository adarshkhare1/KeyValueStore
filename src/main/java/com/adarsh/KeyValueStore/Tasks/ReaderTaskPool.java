package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ReaderTaskPool {
    private static final Logger _LOGGER;
    private static final ExecutorService _WriterPool;
    private static final int _WriteTimeout = 10000; //milliseconds

    static
    {
        _LOGGER = LogManager.getLogger(ReaderTaskPool.class.getName());
        _WriterPool = Executors.newFixedThreadPool(10);

    }

    private final ExecutorCompletionService<StorageBlob> _readCompletionService;
    private final ReadOperation _operation;
    public ReaderTaskPool(ReadOperation operation)
    {
        _operation = operation;
        _readCompletionService = new ExecutorCompletionService<>(_WriterPool);
    }

    public StorageBlob read() throws TimeoutException, StorageException
    {
        List<Future<StorageBlob>> futures = new ArrayList<>();
        for(StoragePartition p:_operation.getPartitions()) {
            ReadTask task = new ReadTask(p, _operation.getKey());
            futures.add(_readCompletionService.submit(task));
            _LOGGER.info("Submitted WriterTask to insert key {} to partition {}.", _operation.getKey(), p);
        }
        return GetValue(_operation.getMinimumSuccessfulReads());
    }


    /**
     * Wait for minimum number of matching reads to return
     * @param minimumMatchReads
     * @return
     * @throws TimeoutException
     * @throws ReadConsistencyException
     */
    private StorageBlob GetValue(int minimumMatchReads) throws TimeoutException, ReadConsistencyException
    {
        long tickCount = currentTimeMillis();
        long timeout = _WriteTimeout;
        StorageBlob result = null;
        int resultCount = 0;
        Map<StorageBlob, Integer> readCountMap = new HashMap<>();
        try {
            while(resultCount < minimumMatchReads && timeout > 0) {
                Future<StorageBlob> resultFuture = _readCompletionService.poll(timeout, MILLISECONDS);
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
                        throw new ReadConsistencyException();
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
