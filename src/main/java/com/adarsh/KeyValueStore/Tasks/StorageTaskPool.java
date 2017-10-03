package com.adarsh.KeyValueStore.Tasks;

import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StorageTaskPool {
    private static final ExecutorService _DeletePool;
    private static final ExecutorService _InsertPool;
    private static final ExecutorService _ReadPool;
    private static final ExecutorService _UpdatePool;

    private static final int _NumberOfThreads = 10;

    static {
        _DeletePool = Executors.newFixedThreadPool(_NumberOfThreads);
        _InsertPool = Executors.newFixedThreadPool(_NumberOfThreads);
        _ReadPool = Executors.newFixedThreadPool(_NumberOfThreads);
        _UpdatePool = Executors.newFixedThreadPool(_NumberOfThreads);
    }

    private StorageTaskPool(){
        //Making class as singleton.
    }

    /**
     * @param operation
     * @return
     */
    public static<T> ExecutorCompletionService<T> CreateNewTaskCompletionService(StorageOperation operation){
        StorageAction action = operation.getStorageAction();
        if(action == StorageAction.Delete) {
            return new ExecutorCompletionService<T>(_DeletePool);
        }
        else if(action == StorageAction.Insert){
            return new ExecutorCompletionService<T>(_InsertPool);
        }
        else if(action == StorageAction.Read){
            return new ExecutorCompletionService<T>(_ReadPool);
        }
        else if(action == StorageAction.Update){
            return new ExecutorCompletionService<T>(_UpdatePool);
        }
        throw new IllegalArgumentException("Unknown Storage Action.");
    }
}
