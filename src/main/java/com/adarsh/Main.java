package com.adarsh;

import com.adarsh.KeyValueStore.Cluster.NodeEndpoint;
import com.adarsh.KeyValueStore.Cluster.StorageMasterNode;
import com.adarsh.KeyValueStore.Cluster.StorageNode;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;

import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) {
        try {
            NodeEndpoint masterEP = new NodeEndpoint("localhost");
            NodeEndpoint storageEP = new NodeEndpoint("localhost");
            StorageMasterNode master = StorageMasterNode.createSingleNodeStorage(masterEP);
            master.registerNewPhysicalNode(storageEP);
            System.out.println(master.toString());
            System.out.println("---List of Storage Nodes---");
            for(StorageNode n: master.getPhysicalStorageNodes()){
                System.out.println(n.toString());
            }
            StorageBlob data = new StorageBlob(("test").getBytes());
            master.insert(1, data);
            byte[] resultData = master.getValue(1).getBlobData();
            System.out.println(new String(resultData));

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (StorageException e) {
            e.printStackTrace();
        } catch (TimeoutException e)
        {
            e.printStackTrace();
        }
    }
}
