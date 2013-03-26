package com.ceph;

import com.ceph.Rados;
import com.ceph.RadosException;
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
}
