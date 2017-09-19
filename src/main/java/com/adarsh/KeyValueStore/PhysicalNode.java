package com.adarsh.KeyValueStore;


import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class PhysicalNode {


    NodeEndpoint _endpoint;


    /**
     * Create a physical node with full key range. It should be used when adding first node in storage system.
     * @param endpoint
     */
    public PhysicalNode(NodeEndpoint endpoint) {
        Preconditions.checkNotNull(endpoint != null, "Endpoint cannot be null.");
        _endpoint = endpoint;

    }

    public NodeEndpoint getEndpoint() {
        return _endpoint;
    }

    public void setEndpoint(NodeEndpoint endpoint) {
        _endpoint = endpoint;
    }


}
