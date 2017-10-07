package com.adarsh.KeyValueStore.Storage;

/**
 * A holder class to the byte array that can be used as storage
 * value in keyvalue store.
 */
public class StorageBlob {

    private final byte[] _blobData;


    private final long _version;

    /**
     * Create an instance of blob with version 0.
     * @param data
     */
    public StorageBlob(byte[] data) {
        this(data, 0);
    }

    /**
     * Create a blob with given version.
     * @param data
     * @param version
     */
    public StorageBlob(byte[] data, long version){
        _blobData = data;
        _version = version;
    }

    /**
     * @return the byte array of the data in the blob.
     */
    public byte[] getBlobData() {
        return _blobData;
    }

    /**
     * @return the version of the blob.
     */
    public long getVersion(){ return _version; }

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
