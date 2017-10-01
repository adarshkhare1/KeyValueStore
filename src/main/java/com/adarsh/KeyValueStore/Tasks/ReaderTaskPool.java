package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
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

    public StorageBlob read() throws InterruptedException
    {
        List<Future<StorageBlob>> futures = new ArrayList<>();
        for(StoragePartition p:_operation.getPartitions()) {
            ReadTask task = new ReadTask(p, _operation.getKey());
            futures.add(_readCompletionService.submit(task));
            _LOGGER.info("Submitted WriterTask to insert key {} to partition {}.", _operation.getKey(), p);
        }
        return GetMinimumReads(_operation.getMinimumSuccessfulReads());
    }

    /**
     * Wait for minimum number of reads.
     * TODO: Currently simply get the minimum reads and return last one. Need to add matching logic to get latest version.
     * @param minReads
     * @return
     * @throws InterruptedException
     */
    private StorageBlob GetMinimumReads(int minReads) throws InterruptedException
    {
        long tickCount = currentTimeMillis();
        long timeout = _WriteTimeout;
        int successCount = 0;
        StorageBlob result = null;
        while(successCount < minReads && timeout > 0) {
            Future<StorageBlob> resultFuture = _readCompletionService.poll(timeout, MILLISECONDS);
            try {
                result = resultFuture.get();
                successCount++;
                long newTickCount = currentTimeMillis();
                timeout = timeout - (newTickCount - tickCount);
                tickCount = newTickCount;
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
