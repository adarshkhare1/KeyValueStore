package com.adarsh.KeyValueStore.Cluster;


import com.adarsh.KeyValueStore.Cluster.NodeEndpoint;
import com.google.common.base.Preconditions;

public abstract class PhysicalNode {


    NodeEndpoint _endpoint;
    String _name;


    /**
     * Create a physical node with full key range. It should be used when adding first node in storage system.
     * @param endpoint
     */
    public PhysicalNode(String name,NodeEndpoint endpoint) {
        Preconditions.checkNotNull(name, "No node name specified");
        Preconditions.checkNotNull(endpoint != null, "Endpoint cannot be null.");
        _endpoint = endpoint;
        _name = name;

    }

    /**
     * @return
     */
    public NodeEndpoint getEndpoint() {
        return _endpoint;
    }


    /**
     * @return
     */
    public String getName() {return _name; }

    @Override
    public String toString(){
        return "Node:"+_name+"->"+_endpoint.toString();
    }


}
