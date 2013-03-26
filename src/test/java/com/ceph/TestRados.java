package com.ceph;

import com.ceph.Rados;
import com.ceph.RadosException;
import java.io.File;
import junit.framework.*;

public final class TestRados extends TestCase {

    File configFile;

    public void setUp() {
        this.configFile = new File("/etc/ceph/ceph.conf");
    }

    /**
        This test verifies if we can get the version out of librados
        It's currently hardcoded to expect at least 0.49.0
     */
    public void testGetVersion() {
        Rados r = new Rados("admin");
        int[] version = r.getVersion();
        assertTrue(version[0] >= 0);
        assertTrue(version[1] >= 49);
        assertTrue(version[2] >= 0);
    }

    public void testGetConfSetGet() {
        try {

            Rados r = new Rados("admin");

            String mon_host = "localhost";
            r.confSet("mon_host", mon_host);
            assertEquals(mon_host, r.confGet("mon_host"));

            String key = "AQDhA0ZRyMK+BhAAvWjWigxVVBGfP7kQzWKjOw==";
            r.confSet("key", key);
            assertEquals(key, r.confGet("key"));

        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }
}
