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

package com.ceph.rados;

import com.ceph.rados.Rados;
import com.ceph.rados.RadosException;
import com.ceph.rados.jna.RadosClusterInfo;
import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.jna.RadosPoolInfo;
import com.ceph.rados.IoCTX;
import java.io.File;
import java.lang.IllegalArgumentException;
import junit.framework.*;

public final class TestRados extends TestCase {

    /**
        All these variables can be overwritten, see the setUp() method
     */
    String configFile = "/etc/ceph/ceph.conf";
    String id = "admin";
    String pool = "data";

    /**
        This test reads it's configuration from the environment
        Possible variables:
        * RADOS_JAVA_ID
        * RADOS_JAVA_CONFIG_FILE
        * RADOS_JAVA_POOL
     */
    public void setUp() {
        if (System.getenv("RADOS_JAVA_CONFIG_FILE") != null) {
            this.configFile = System.getenv("RADOS_JAVA_CONFIG_FILE");
        }

        if (System.getenv("RADOS_JAVA_ID") != null) {
            this.id = System.getenv("RADOS_JAVA_ID");
        }

        if (System.getenv("RADOS_JAVA_POOL") != null) {
            this.pool = System.getenv("RADOS_JAVA_POOL");
        }
    }

    /**
        This test verifies if we can get the version out of librados
        It's currently hardcoded to expect at least 0.48.0
     */
    public void testGetVersion() {
        Rados r = new Rados(this.id);
        int[] version = r.getVersion();
        assertTrue(version[0] >= 0);
        assertTrue(version[1] >= 48);
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
            long id = r.poolLookup(this.pool);
            assertTrue("The pool ID should be at least 0", id >= 0);

            String name = r.poolReverseLookup(id);
            assertEquals("The pool names didn't match!", this.pool, name);

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
            IoCTX io = r.ioCtxCreate(this.pool);
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
            IoCTX io = r.ioCtxCreate(this.pool);

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

            IoCTX io = r.ioCtxCreate(this.pool);

            assertEquals(this.pool, io.getPoolName());

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    /**
     * This is an pretty extensive test which creates an object
     * writes data, appends, truncates verifies the written data
     * and finally removes the object
     */
    public void testIoCtxWriteListAndRead() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();

            IoCTX io = r.ioCtxCreate(this.pool);

            /**
             * The object we will write to with the data
             */
            String oid = "rados-java";
            String content = "junit wrote this";

            io.write(oid, content);

            String[] objects = io.listObjects();
            assertTrue("We expect at least one object in the pool", objects.length > 0);

            String buf = io.read(oid, content.length(), 0);
            RadosObjectInfo info = io.stat(oid);

            assertEquals("The object names didn't match", oid, info.getOid());
            assertEquals("The size of what we wrote doesn't match with the stat", content.length(), info.getSize());
            assertEquals("The content we read was different from what we wrote", content, buf);

            long now = System.currentTimeMillis()/1000;
            assertFalse("The mtime was in the future", now < info.getMtime());

            /**
             * We simply append the already written data
             */
            io.append(oid, content);
            assertEquals("The size doesn't match after the append", content.length()*2, io.stat(oid).getSize());

            /**
              * We now resize the object to it's original size
             */
            io.truncate(oid, content.length());
            assertEquals("The size doesn't match after the truncate", content.length(), io.stat(oid).getSize());

            io.remove(oid);

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testIoCtxPoolStat() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();

            IoCTX io = r.ioCtxCreate(this.pool);

            RadosPoolInfo info = io.poolStat();
            assertTrue(info.num_objects >= 0);

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testIoCtxSnapshot() {
        try {

            String snapname = "my-new-snapshot";

            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();

            IoCTX io = r.ioCtxCreate(this.pool);

            io.snapCreate(snapname);

            long snapid = io.snapLookup(snapname);
            long time = io.snapGetStamp(snapid);
            String snapnamebuf = io.snapGetName(snapid);

            Long[] snaps = io.snapList();

            io.snapRemove(snapname);

            assertTrue("There should at least be one snapshot", snaps.length >= 1);
            assertEquals("The snapshot names didn't match", snapname, snapnamebuf);

            long now = System.currentTimeMillis()/1000;
            /* Add 5 seconds to deal with clock differences */
            assertTrue("The timestamp was in the future. Clocks synced?", (now + 5) >= time);

            r.ioCtxDestroy(io);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

}
