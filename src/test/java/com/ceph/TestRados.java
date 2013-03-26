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
import com.ceph.RadosClusterStructure;
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

    public void testGetFsid() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            assertNotNull("The fsid returned was null", r.getFsid());
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }

    public void testClusterStat() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            RadosClusterStructure stat = r.clusterStat();
            assertTrue("Cluster size wasn't greater than 0", stat.kb > 0);
            assertTrue("KB used was not 0 or greater", stat.kb_used >= 0);
            assertTrue("KB available was not greater than 0", stat.kb_avail > 0);
            assertTrue("Number of objects was not 0 or greater", stat.num_objects >= 0);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }
}
