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

public class UpdateOperation extends StorageOperation<StorageResult> {
    private static final Logger _LOGGER;
    private static final int _UpdateTimeout = 10000; //milliseconds

    static
    {
        _LOGGER = LogManager.getLogger(UpdateOperation.class.getName());
    }

    private int _minimumSuccessfulUpdates = 1;
    private StorageBlob _value;

    public int getMinimumSuccessfulUpdates()
    {
        return _minimumSuccessfulUpdates;
    }

    public void setMinimumSuccessfulUpdates(int value)
    {
        this._minimumSuccessfulUpdates = value;
    }

    public StorageBlob getValue() {
        return _value;
    }

    public void setValue(StorageBlob value) {
        this._value = value;
    }

    @Override
    public StorageAction getStorageAction() {
        return StorageAction.Update;
    }

    @Override
    public StorageResult execute() throws TimeoutException, StorageException
    {
        ExecutorCompletionService<StorageResult> executionPool
                = StorageTaskPool.CreateNewTaskCompletionService(this);
        List<Future<StorageResult>> futures = new ArrayList<>();
        for (StoragePartition p : super.getPartitions())
        {
            UpdateTask task = new UpdateTask(p, super.getKey(), _value);
            futures.add(executionPool.submit(task));
            _LOGGER.info("Submitted WriterTask to insert key {} to partition {}.", super.getKey(), p);
        }
        super.waitForMinimumWrites(executionPool,
                _minimumSuccessfulUpdates,
                _UpdateTimeout,
                _LOGGER);
        return StorageResult.Success;
    }
}
