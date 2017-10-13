package com.adarsh.KeyValueStore.Transport;

import java.io.*;
import java.net.Socket;

public class StorageRequestClient {
    private final InputStream inStream;
    private final OutputStream outStream;

    public StorageRequestClient(Socket clientSocket){
        try {
            inStream = clientSocket.getInputStream();
            outStream = clientSocket.getOutputStream();
        } catch (IOException e) {
           throw new IllegalArgumentException(e);
        }
    }

    public void start(){

    }

    public KeyValueStoreRequest getNextRequest(){
        return null;
    }

    public void shutDown(){
        try {
            inStream.close();
            outStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
