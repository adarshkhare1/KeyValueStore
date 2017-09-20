package com.adarsh;

import com.adarsh.KeyValueStore.Cluster.NodeEndpoint;
import com.adarsh.KeyValueStore.Cluster.StorageMaster;
import com.adarsh.KeyValueStore.Cluster.StorageNode;

import java.net.UnknownHostException;

public class Main {

    public static void main(String[] args) {
        try {
            NodeEndpoint masterEP = new NodeEndpoint("localhost");
            NodeEndpoint storageEP = new NodeEndpoint("localhost");
            StorageMaster master = StorageMaster.createSingleNodeStorage(masterEP);
            master.registerNewPhysicalNode(storageEP);
            System.out.println(master.toString());
            System.out.println("---List of Storage Nodes---");
            for(StorageNode n: master.getPhysicalStorageNodes()){
                System.out.println(n.toString());
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
