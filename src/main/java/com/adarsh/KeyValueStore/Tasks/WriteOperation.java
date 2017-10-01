package com.adarsh.KeyValueStore.Tasks;

import com.adarsh.KeyValueStore.Storage.StorageBlob;

public class WriteOperation extends StorageOperation {
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

}
