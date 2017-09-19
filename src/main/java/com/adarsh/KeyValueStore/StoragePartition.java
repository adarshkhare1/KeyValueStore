package com.adarsh.KeyValueStore;

public class StoragePartition {
    private KeyRange _keyRange;

    public StoragePartition(){
       this(new KeyRange());
    }
    public StoragePartition(KeyRange keyRange){
        _keyRange = keyRange;
    }
}
