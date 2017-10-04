package com.adarsh.KeyValueStore.Cluster.Test;

import com.adarsh.KeyValueStore.Cluster.NodeEndpoint;
import com.adarsh.KeyValueStore.Cluster.StorageMasterNode;
import com.adarsh.KeyValueStore.Cluster.StorageNode;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

class StorageMasterNodeTest {
    @Test
    void insert() {
        try {
            StorageMasterNode master = buildStorageNode();
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

    @Test
    void createSingleNodeStorage() {
        try {
            StorageMasterNode master = buildStorageNode();
            Assertions.assertEquals("Node:_master_->localhost/127.0.0.1:16000", master.toString(), "master.toString()");
            Assertions.assertEquals(1, master.getPhysicalStorageNodes().length, "Storage node count" );
        } catch (UnknownHostException e) {
            Assertions.fail(e);
        }
    }
    private StorageMasterNode buildStorageNode() throws UnknownHostException {
        NodeEndpoint masterEP = new NodeEndpoint("localhost");
        NodeEndpoint storageEP = new NodeEndpoint("localhost");
        StorageMasterNode master = StorageMasterNode.createSingleNodeStorage(masterEP);
        master.registerNewPhysicalNode(storageEP);
        Assertions.assertEquals("Node:_master_->localhost/127.0.0.1:16000", master.toString(), "master.toString()");
        Assertions.assertEquals(1, master.getPhysicalStorageNodes().length, "Storage node count" );
        return master;
    }
}