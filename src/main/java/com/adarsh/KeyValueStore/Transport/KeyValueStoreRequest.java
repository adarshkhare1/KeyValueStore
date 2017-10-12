package com.adarsh.KeyValueStore.Transport;

import com.adarsh.KeyValueStore.Tasks.StorageAction;

/**
 * Represent the wire format of the request
 */
public class KeyValueStoreRequest {
    private StorageAction _operation;
    private long _key;
    private byte[] _data;

    public StorageAction getOperation() {
        return _operation;
    }

    public void setOperation(StorageAction operation) {
        this._operation = _operation;
    }

    public long getKey() {
        return _key;
    }

    public void setKey(long key) {
        this._key = _key;
    }

    public byte[] getData() {
        return _data;
    }

    public void setData(byte[] data) {
        this._data = _data;
    }
}
