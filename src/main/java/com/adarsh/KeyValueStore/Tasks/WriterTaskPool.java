package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StoragePartition;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class WriterTaskPool{
    private static final Logger _LOGGER;
    private static final ExecutorService _WriterPool;
    private static final int _WriteTimeout = 10000; //milliseconds

    static
    {
        _LOGGER = LogManager.getLogger(WriterTaskPool.class.getName());
        _WriterPool = Executors.newFixedThreadPool(10);

    }

    private final ExecutorCompletionService<StorageResult> _writeCompletionService;
    private final WriteOperation _operation;
    public WriterTaskPool(WriteOperation operation)
    {
        Preconditions.checkNotNull(operation);
        _operation = operation;
        _writeCompletionService = new ExecutorCompletionService<>(_WriterPool);
    }

    public void write() throws InterruptedException
    {
        List<Future<StorageResult>> futures = new ArrayList<>();
        for(StoragePartition p:_operation.getPartitions()) {
            WriteTask task = new WriteTask(p, _operation.getKey(), _operation.getValue());
            futures.add(_writeCompletionService.submit(task));
            _LOGGER.info("Submitted WriterTask to insert key {} to partition {}.", _operation.getKey(), p);
        }
        waitForMinimumWrites(_operation.getMinimumSuccessfulWrites());
    }

    private void waitForMinimumWrites(int minWrites) throws InterruptedException
    {
        long tickCount = currentTimeMillis();
        long timeout = _WriteTimeout;
        int successCount = 0;
        while(successCount < minWrites && timeout > 0) {
            Future<StorageResult> resultFuture = _writeCompletionService.poll(timeout, MILLISECONDS);
            try {
                if(resultFuture.get() == StorageResult.Success) successCount++;
                long newTickCount = currentTimeMillis();
                timeout = timeout - (newTickCount - tickCount);
                tickCount = newTickCount;
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
