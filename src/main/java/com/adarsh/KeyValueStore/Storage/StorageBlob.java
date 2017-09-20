package com.adarsh.KeyValueStore.Storage;

public class StorageBlob {

    private final byte[] _blobData;

    public StorageBlob(byte[] data){
        _blobData = data;
    }

    public byte[] getBlobData() {
        return _blobData;
    }

}
