package com.adarsh;

import com.adarsh.KeyValueStore.NodeEndpoint;
import com.adarsh.KeyValueStore.StorageMaster;

public class Main {

    public static void main(String[] args) {
        NodeEndpoint masterEP = new NodeEndpoint();
        NodeEndpoint storageEP = new NodeEndpoint();
        StorageMaster master = StorageMaster.createSingleNodeStorage(masterEP);
        master.addNewPhysicalNode(storageEP);
	    System.out.println(master.toString());
    }
}
