package com.adarsh.KeyValueStore.Storage;

/**
 * A holder class to the byte array that can be used as storage
 * value in keyvalue store.
 */
public class StorageBlob {

    private final byte[] _blobData;

    public StorageBlob(byte[] data){
        _blobData = data;
    }

    public byte[] getBlobData() {
        return _blobData;
    }

    @Override
    public int hashCode() {
        return _blobData.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != StorageBlob.class){
            return false;
        }
        else {
            return (_blobData == ((StorageBlob) obj).getBlobData());
        }
    }
}
