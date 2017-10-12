package com.adarsh.KeyValueStore.Transport;

import com.adarsh.KeyValueStore.Tasks.StorageAction;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public class WireRequestParser {
    private static final char _RecordSeparator =',';
    public static KeyValueStoreRequest ParseRequest(String reqString){
        Preconditions.checkNotNull(reqString);
        StorageAction action = null;
        long key = Long.MIN_VALUE;
        byte[] data = null;

        int index = reqString.indexOf(_RecordSeparator);
        if(index >= 0){
            action = parseStorageAction(reqString.substring(0, index));
            reqString = reqString.substring(index+1);
        }
        index = reqString.indexOf(_RecordSeparator);
        if(index >= 0){
            key = parseKey(reqString.substring(0, index));
            reqString = reqString.substring(index+1);
        }
        if(!StringUtils.isNotBlank(reqString)) {
            data = reqString.getBytes();
        }
        KeyValueStoreRequest request = buildKeyValueStoreRequest(action, key, data);
        return  request;

    }

    private static StorageAction parseStorageAction(String s) {
        StorageAction action = null;
        switch (s.toUpperCase()){
            case "U":
                action = StorageAction.Update;
                break;
            case "I":
                action = StorageAction.Insert;
                break;
            case "R":
                action = StorageAction.Read;
                break;
            case "D":
                action = StorageAction.Delete;
                break;
        }
        return action;
    }

    private static long parseKey(String s) {
        long key;
        key = Long.parseLong(s);
        return key;
    }

    private static KeyValueStoreRequest buildKeyValueStoreRequest(StorageAction action, long key, byte[] data) {
        KeyValueStoreRequest request = new KeyValueStoreRequest();
        if(action != null) {
            request.setOperation(action);
        }
        if(key != Long.MIN_VALUE) {
            request.setKey(key);
        }
        if(data != null) {
            request.setData(data);
        }
        return request;
    }

}
