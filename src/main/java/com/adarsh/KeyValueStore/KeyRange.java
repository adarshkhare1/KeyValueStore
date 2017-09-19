package com.adarsh.KeyValueStore;

import com.google.common.base.Preconditions;

public class KeyRange {
    private static final int  MIN_KEY = 0;
    private static long MAX_KEY = Long.MAX_VALUE;
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
        this.setStartKey(startKey);
        this.setEndKey(endKey);
    }


    public long getStartKey() { return _startKey; }

    public void setStartKey(long startKey)
    {
        Preconditions.checkArgument(this.isKeyInRange(startKey), "startKey not in range.");
        _startKey = startKey;
    }

    public long getEndKey() {
        return _endKey;
    }

    public void setEndKey(long endKey) {
        Preconditions.checkArgument(this.isKeyInRange(endKey), "endKey not in range.");
        _endKey = endKey;
    }

    public long getKeySpan(){ return (_endKey - _startKey);}

    private boolean isKeyInRange(Long value)
    {
        return (value >= KeyRange.MIN_KEY && value <= KeyRange.MAX_KEY);
    }
}
