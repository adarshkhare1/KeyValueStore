package com.adarsh.KeyValueStore.Tasks;

public class ReadOperation extends StorageOperation {
    private int _minimumSuccessfulReads = 1;

    public int getMinimumSuccessfulReads()
    {
        return _minimumSuccessfulReads;
    }

    public void setMinimumSuccessfulReads(int value)
    {
        this._minimumSuccessfulReads = value;
    }
}
