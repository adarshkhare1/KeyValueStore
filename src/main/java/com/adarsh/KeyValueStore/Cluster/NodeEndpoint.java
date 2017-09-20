package com.adarsh.KeyValueStore.Cluster;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/**
 * Endpoint represent the address for physical node. It is IPAddress:port. Default port is 16000.
 */
public class NodeEndpoint {
    public static final int DEFAULT_PORT=16000;

    private SocketAddress _ipEndpointAddress;

    /**
     * @param fqdn
     * @throws UnknownHostException
     */
    public NodeEndpoint(String fqdn) throws UnknownHostException {
        this(fqdn, DEFAULT_PORT);
    }

    /**
     * @param fqdn
     * @param port
     * @throws UnknownHostException
     */
    public NodeEndpoint(String fqdn, int port) throws UnknownHostException {
        this(InetAddress.getByName(fqdn), port);
    }

    /**
     * @param addr
     * @param port
     */
    public NodeEndpoint(InetAddress addr, int port){
        _ipEndpointAddress = new InetSocketAddress(addr, port);
    }

    /**
     * @return
     */
    public SocketAddress get_ipEndpointAddress() {
        return _ipEndpointAddress;
    }

    @Override
    public String toString() {
        return _ipEndpointAddress.toString();
    }
}
