package com.adarsh.KeyValueStore.Transport;


import com.adarsh.KeyValueStore.Cluster.ReadRequestRouter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class KeyValueStoreListener {
    private static final Logger _LOGGER;
    private static final Object _syncObject;
    private static KeyValueStoreListener _keyStoreServer;
    private static boolean _paused;

    static {
        _LOGGER = LogManager.getLogger(KeyValueStoreListener.class.getName());
        _syncObject = new Object();
        _paused = false;

    }

    private int _DefaultPort = 4400;
    private final ServerSocket _serverSocket;
    private final Map<Socket, StorageRequestClient> _clientList;


    private KeyValueStoreListener() throws IOException {
        _serverSocket = new ServerSocket(_DefaultPort);
        _clientList = new HashMap<>();

    }

    public void acceptConnection() throws IOException {
        _LOGGER.info("Starting the storage server.");
        Socket clientSocket =  _serverSocket.accept();
        _LOGGER.info("Accepted the client connection {}.", clientSocket.getInetAddress().toString());
        StorageRequestClient requestclient = new StorageRequestClient(clientSocket);
        requestclient.start();
        _clientList.put(clientSocket, requestclient);
    }

    /**
     * Close the servwr socket and all connected client sockets.
     * @throws IOException
     */
    public void shutdown() {
        _LOGGER.info("SHutting down the storage server.");
        try {
            _serverSocket.close();
        } catch (IOException e) {
            _LOGGER.warn(e.getStackTrace());
        }
        _LOGGER.info("SHutting down all connected client connections.");
        for(Map.Entry<Socket,StorageRequestClient > kv : _clientList.entrySet()){
            try {
                _LOGGER.info("SHutting down the client {}.", kv.getKey().getInetAddress().toString());
                kv.getKey().close();
            } catch (IOException e) {
                _LOGGER.warn(e.getStackTrace());
            }
        }
    }

    public static void start() throws Exception {
        try {
            initializeListenerSocket();
            while(!_paused) {
                _keyStoreServer.acceptConnection();
            }

        } catch (IOException e) {
            _LOGGER.warn(e.getStackTrace());
        }
    }

    private static void initializeListenerSocket() throws Exception {
        if(_keyStoreServer == null){
            synchronized (_syncObject){
                if(_keyStoreServer == null){
                    _keyStoreServer = new KeyValueStoreListener();
                }
                else{
                    throw new Exception("Invalid operation.");
                }
            }
        }
        else{
            throw new Exception("Invalid operation.");
        }
    }

    public static void shutDown(){
        _paused = true;
        _keyStoreServer.shutdown();
    }
}
