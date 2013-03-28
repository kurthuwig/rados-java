/*
 * RADOS Java - Java bindings for librados
 *
 * Copyright (C) 2013 Wido den Hollander <wido@42on.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file LICENSE.
 *
 */

package com.ceph;

import com.ceph.Rados;
import com.ceph.RadosException;
import com.ceph.RadosClusterInfo;
import com.ceph.IoCTX;
import java.io.File;
import junit.framework.*;

public final class TestRados extends TestCase {

    /**
        All these variables can be overwritten, see the setUp() method
     */
    String configFile = "/etc/ceph/ceph.conf";
    String id = "admin";

    /**
        This test reads it's configuration from the environment
        Possible variables:
        * RADOS_JAVA_ID
        * RADOS_JAVA_CONFIG_FILE
     */
    public void setUp() {
        if (System.getenv("RADOS_JAVA_CONFIG_FILE") != null) {
            this.configFile = System.getenv("RADOS_JAVA_CONFIG_FILE");
        }

        if (System.getenv("RADOS_JAVA_ID") != null) {
            this.id = System.getenv("RADOS_JAVA_ID");
        }
    }

    /**
        This test verifies if we can get the version out of librados
        It's currently hardcoded to expect at least 0.49.0
     */
    public void testGetVersion() {
        Rados r = new Rados(this.id);
        int[] version = r.getVersion();
        assertTrue(version[0] >= 0);
        assertTrue(version[1] >= 49);
        assertTrue(version[2] >= 0);
    }

    public void testGetConfSetGet() {
        try {
            Rados r = new Rados(this.id);

            String mon_host = "127.0.0.1";
            r.confSet("mon_host", mon_host);
            assertEquals(mon_host, r.confGet("mon_host"));

            String key = "mySuperSecretKey";
            r.confSet("key", key);
            assertEquals(key, r.confGet("key"));
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testConnect() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testClusterFsid() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            assertNotNull("The fsid returned was null", r.clusterFsid());
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testClusterStat() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            RadosClusterInfo stat = r.clusterStat();
            assertTrue("Cluster size wasn't greater than 0", stat.kb > 0);
            assertTrue("KB used was not 0 or greater", stat.kb_used >= 0);
            assertTrue("KB available was not greater than 0", stat.kb_avail > 0);
            assertTrue("Number of objects was not 0 or greater", stat.num_objects >= 0);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testPoolList() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            String[] pools = r.poolList();
            assertNotNull(pools);
            assertTrue("We expect at least 3 pools (data, metadata, rbd)", pools.length >= 3);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testPoolLookup() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            long id = r.poolLookup("data");
            assertTrue("The pool ID should be at least 0", id >= 0);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testInstanceId() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            long id = r.getInstanceId();
            assertTrue("The id should be greater than 0", id > 0);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testIoCtxCreateAndDestroyWithID() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            IoCTX io = r.ioCtxCreate("data");
            long id = io.getId();
            assertTrue("The pool ID should be at least 0", id >= 0);
            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testIoCtxGetSetAuid() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            IoCTX io = r.ioCtxCreate("data");

            /**
               We fetch the auid, try to set it to 42 and set it
               back again to the original value
            */
            long auid = io.getAuid();
            assertTrue("The auid should be at least 0", auid >= 0);

            io.setAuid(42);
            assertEquals("The auid should be 42", 42, io.getAuid());

            io.setAuid(auid);
            assertEquals("The auid should be 0", 0, io.getAuid());

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testIoCtxPoolName() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();

            String poolName = "data";
            IoCTX io = r.ioCtxCreate(poolName);

            assertEquals(poolName, io.getPoolName());

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testIoCtxListObjects() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();

            String poolName = "data";
            IoCTX io = r.ioCtxCreate(poolName);

            String[] objects = io.listObjects();
            for (int i = 0; i < objects.length; i++) {
                System.out.println(objects[i]);
            }

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testIoCtxWriteAndRead() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();

            String poolName = "data";
            IoCTX io = r.ioCtxCreate(poolName);

            String oid = "rados-java";
            String content = "junit wrote this";
            io.write(oid, content);

            String buf = io.read(oid, content.length(), 0);

            assertEquals("The content we read was different from what we wrote", content, buf);

            io.remove(oid);

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

}
