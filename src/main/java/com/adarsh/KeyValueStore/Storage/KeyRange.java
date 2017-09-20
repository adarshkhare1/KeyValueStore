package com.adarsh.KeyValueStore.Storage;

import com.google.common.base.Preconditions;

public class KeyRange {
    private static final long  MIN_KEY = 0;
    private static final long MAX_KEY = Long.MAX_VALUE;
    private long _startKey;
    private long _endKey;

    /**
     * Default constructor for keyrange
     */
    public KeyRange(){
        this(MIN_KEY, MAX_KEY);
    }

    public KeyRange(long startKey, long endKey)
    {
        Preconditions.checkArgument(this.isValidKeyValue(startKey), "startKey not in range.");
        Preconditions.checkArgument(this.isValidKeyValue(endKey), "endKey not in range.");
        _startKey = startKey;
        _endKey = endKey;
    }


    public long getStartKey() { return _startKey; }

    public long getEndKey() {
        return _endKey;
    }

    public long getKeySpan(){ return (_endKey - _startKey);}

    public void verifyIfKeyIsInRange(long key) throws KeyOutOfRangeException {
        if(key < _startKey || key > _endKey)
            throw new KeyOutOfRangeException();
    }

    private boolean isValidKeyValue(Long value)
    {
        return (value >= KeyRange.MIN_KEY && value <= KeyRange.MAX_KEY);
    }
}
