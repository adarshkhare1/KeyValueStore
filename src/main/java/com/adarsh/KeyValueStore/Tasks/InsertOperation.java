package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import com.adarsh.KeyValueStore.Storage.StoragePartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InsertOperation extends StorageOperation<StorageResult> {

    private static final Logger _LOGGER;
    private static final int _WriteTimeout = 10000; //milliseconds

    static
    {
        _LOGGER = LogManager.getLogger(InsertOperation.class.getName());
    }

    private int _minimumSuccessfulWrites = 1;
    private StorageBlob _value;

    public int getMinimumSuccessfulWrites()
    {
        return _minimumSuccessfulWrites;
    }

    public void setMinimumSuccessfulWrites(int value)
    {
        this._minimumSuccessfulWrites = value;
    }

    public StorageBlob getValue() {
        return _value;
    }

    public void setValue(StorageBlob value) {
        this._value = value;
    }

    @Override
    public StorageAction getStorageAction() {
        return StorageAction.Insert;
    }

    @Override
    public StorageResult execute() throws TimeoutException, StorageException {
        ExecutorCompletionService<StorageResult> executionPool
                = StorageTaskPool.CreateNewTaskCompletionService(this);
        List<Future<StorageResult>> futures = new ArrayList<>();
        for (StoragePartition p : super.getPartitions())
        {
            InsertTask task = new InsertTask(p, super.getKey(), _value);
            futures.add(executionPool.submit(task));
            _LOGGER.info("Submitted WriterTask to insert key {} to partition {}.", super.getKey(), p);
        }
        waitForMinimumWrites(executionPool, _minimumSuccessfulWrites);
        return StorageResult.Success;
    }

    private void waitForMinimumWrites(ExecutorCompletionService<StorageResult> executionPool,
                                      int minWrites) throws TimeoutException
    {
        long tickCount = currentTimeMillis();
        long timeout = _WriteTimeout;
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
                    _LOGGER.warn(e.getMessage());
                    throw new TimeoutException();
                }
            }
        } catch (InterruptedException e)
        {
            _LOGGER.warn(e.getMessage());
            throw new TimeoutException();
        }
    }
}
