package com.adarsh.KeyValueStore.Cluster.Test;

import com.adarsh.KeyValueStore.Cluster.NodeEndpoint;
import com.adarsh.KeyValueStore.Cluster.StorageMasterNode;
import com.adarsh.KeyValueStore.Storage.KeyNotFoundException;
import com.adarsh.KeyValueStore.Storage.StorageBlob;
import com.adarsh.KeyValueStore.Storage.StorageException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

class StorageMasterNodeTest {
    private static final String _TestString = "test";
    private final StorageMasterNode _master = buildStorageNode();

    public StorageMasterNodeTest() throws UnknownHostException {
        StorageMasterNode _master = buildStorageNode();
    }

    @Test
    void insert() {
        try {
            StorageBlob data = new StorageBlob((_TestString).getBytes());
            _master.insert(1, data);
            byte[] resultData = _master.getValue(1).getBlobData();
            Assertions.assertEquals(_TestString, new String(resultData), "data after insert");

        } catch (StorageException e) {
            e.printStackTrace();
            Assertions.fail("Key not deleted");
        } catch (TimeoutException e)
        {
            e.printStackTrace();
            Assertions.fail("Key not deleted");
        }
    }

    @Test
    void update() {
        try {
            StorageBlob data = new StorageBlob((_TestString).getBytes());
            _master.insert(1, data);
            StorageBlob result = _master.getValue(1);
            Assertions.assertEquals(_TestString, new String(result.getBlobData()), "data after insert");
            Assertions.assertEquals(0, result.getVersion(), "version after insert");
            //Update the key 1
            String newString = _TestString+"2";
            long newVersion = data.getVersion()+1;
            data = new StorageBlob(newString.getBytes(), newVersion);
            _master.update(1, data);
            result = _master.getValue(1);
            Assertions.assertEquals(newString, new String(result.getBlobData()), "data after update");
            Assertions.assertEquals(newVersion, result.getVersion(), "version after update");

        } catch (StorageException e) {
            e.printStackTrace();
            Assertions.fail("Key not deleted");
        } catch (TimeoutException e)
        {
            e.printStackTrace();
            Assertions.fail("Key not deleted");
        }
    }

    @Test
    void delete() {
        try {
            StorageBlob data = new StorageBlob(("test").getBytes());
            _master.insert(1, data);
            byte[] resultData = _master.getValue(1).getBlobData();
            Assertions.assertEquals(_TestString, new String(resultData), "data after insert");
            _master.delete(1);
            try{
                _master.getValue(1).getBlobData();
                Assertions.fail("Key not deleted");
            }
            catch (KeyNotFoundException ex){
              //Key deleted successfully if we get this exception.
            }

        } catch (StorageException e) {
            e.printStackTrace();
            Assertions.fail("Key not deleted");
        } catch (TimeoutException e)
        {
            e.printStackTrace();
            Assertions.fail("Key not deleted");
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