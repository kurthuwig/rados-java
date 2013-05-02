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

package com.ceph.rbd;

import com.ceph.rbd.Rbd;
import com.ceph.rbd.RbdException;
import com.ceph.rados.Rados;
import com.ceph.rados.RadosException;
import com.ceph.rados.IoCTX;
import java.io.File;
import java.lang.IllegalArgumentException;
import junit.framework.*;

public final class TestRbd extends TestCase {

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
        int[] version = Rbd.getVersion();
        assertTrue(version[0] >= 0);
        assertTrue(version[1] >= 1);
        assertTrue(version[2] >= 8);
    }

    public void testCreateListAndRemoveImage() {
        try {
            Rados r = new Rados(this.id);
            r.confReadFile(new File(this.configFile));
            r.connect();
            IoCTX io = r.ioCtxCreate(this.pool);

            String imageName = "testimage1";

            Rbd rbd = new Rbd(io);
            rbd.create(imageName, 10485760);

            String[] images = rbd.list();
            assertTrue("There were no images in the pool", images.length > 0);

            rbd.remove(imageName);

            r.ioCtxDestroy(io);
        } catch (RbdException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
    }
}