/*
 * RADOS Java - Java bindings for librados
 *
 * Copyright (C) 2013 Wido den Hollander <wido@42on.com>
 * Copyright (C) 2014 1&1 - Behar Veliqi <behar.veliqi@1und1.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.ceph.rados;

import com.ceph.rados.ReadOp.ReadResult;
import com.ceph.rados.exceptions.RadosException;
import com.ceph.rados.jna.RadosClusterInfo;
import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.jna.RadosPoolInfo;
import com.sun.jna.Pointer;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class TestRados {

    private static String ENV_CONFIG_FILE = System.getenv("RADOS_JAVA_CONFIG_FILE");
    private static String ENV_ID = System.getenv("RADOS_JAVA_ID");
    private static String ENV_POOL = System.getenv("RADOS_JAVA_POOL");

    private static final String CONFIG_FILE = ENV_CONFIG_FILE == null ? "/etc/ceph/ceph.conf" : ENV_CONFIG_FILE;
    private static final String ID = ENV_ID == null ? "admin" : ENV_ID;
    private static final String POOL = ENV_POOL == null ? "data" : ENV_POOL;

    private static Rados rados;
    private static IoCTX ioctx;


    @BeforeClass
    public static void setUp() throws Exception {
        rados = new Rados(ID);
        rados.confReadFile(new File(CONFIG_FILE));
        rados.connect();
        ioctx = rados.ioCtxCreate(POOL);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        rados.shutDown();
        rados.ioCtxDestroy(ioctx);
    }

    /**
     * This test verifies if we can get the version out of librados
     * It's currently hardcoded to expect at least 0.48.0
     */
    @Test
    public void testGetVersion() {
        int[] version = Rados.getVersion();
        assertTrue(version[0] >= 0);
        assertTrue(version[1] >= 48);
        assertTrue(version[2] >= 0);
    }


    @Test
    public void testGetConfSetGet() throws Exception {
        Rados rados = new Rados(ID);

        String mon_host = "127.0.0.1";
        rados.confSet("mon_host", mon_host);
        assertEquals(mon_host, rados.confGet("mon_host"));

        String key = "mySuperSecretKey";
        rados.confSet("key", key);
        assertEquals(key, rados.confGet("key"));
    }

    @Test(expected = RadosException.class)
    public void testConfSetInConnectedState() throws Exception {
        rados.confSet("mon_host", "host.server.lan");
    }

    @Test
    public void testClusterFsid() throws Exception {
        assertNotNull("The fsid returned was null", rados.clusterFsid());
    }


    @Test
    public void testClusterStat() throws Exception {
        RadosClusterInfo stat = rados.clusterStat();
        assertTrue("Cluster size wasn't greater than 0", stat.kb > 0);
        assertTrue("KB used was not 0 or greater", stat.kb_used >= 0);
        assertTrue("KB available was not greater than 0", stat.kb_avail > 0);
        assertTrue("Number of objects was not 0 or greater", stat.num_objects >= 0);
    }


    @Test
    public void testPoolList() throws Exception {
        String[] pools = rados.poolList();
        assertNotNull(pools);
        assertTrue("We expect at least 1 pool (the one using here)", pools.length >= 1);
    }


    @Test
    public void testPoolLookup() throws Exception {
        long id = rados.poolLookup(POOL);
        assertTrue("The POOL ID should be at least 0", id >= 0);

        String name = rados.poolReverseLookup(id);
        assertEquals("The POOL names didn't match!", POOL, name);
    }

    @Test
    public void testInstanceId() throws Exception {
        long id = rados.getInstanceId();
        assertTrue("The id should be greater than 0", id > 0);
    }

    @Test
    public void testIoCtxCreateAndDestroyWithID() throws Exception {
        long id = ioctx.getId();
        assertTrue("The POOL ID should be at least 0", id >= 0);
    }

    @Test
    public void testIoCtxGetSetAuid() throws Exception {
        long auid = ioctx.getAuid();

        ioctx.setAuid(42);
        assertEquals("The auid should be 42", 42, ioctx.getAuid());

        ioctx.setAuid(auid); // reset to original
    }

    @Test
    public void testIoCtxPoolName() throws Exception {
        assertEquals(POOL, ioctx.getPoolName());
    }

    /**
     * This is an pretty extensive test which creates an object
     * writes data, appends, truncates verifies the written data
     * and finally removes the object
     */
    @Test
    public void testIoCtxWriteListAndRead() throws Exception {
        /**
         * The object we will write to with the data
         */
        String oid = "rados-java";
        byte[] content = "junit wrote this".getBytes();

        ioctx.write(oid, content);

        String[] objects = ioctx.listObjects();
        assertTrue("We expect at least one object in the POOL", objects.length > 0);

        verifyDocument(oid, content);

        /**
         * We simply append the already written data
         */
        ioctx.append(oid, content);
        assertEquals("The size doesn't match after the append", content.length * 2, ioctx.stat(oid).getSize());

        /**
         * We now resize the object to it's original size
         */
        ioctx.truncate(oid, content.length);
        assertEquals("The size doesn't match after the truncate", content.length, ioctx.stat(oid).getSize());

        ioctx.remove(oid);
    }

    /**
     * This test creates an object, appends some data and removes it afterwards
     */
    @Test
    public void testIoCtxWriteAndAppendBytes() throws Exception {
        /**
         * The object we will write to with the data
         */
        String oid = "rados-java";

        try {
            byte[] buffer = new byte[20];
            // use a fix seed so that we always get the same data
            new Random(42).nextBytes(buffer);

            ioctx.write(oid, buffer);

            /**
             * We simply append the parts of the already written data
             */
            ioctx.append(oid, buffer, buffer.length / 2);

            int expectedFileSize = buffer.length + buffer.length / 2;
            assertEquals("The size doesn't match after the append", expectedFileSize, ioctx.stat(oid).getSize());

            byte[] readBuffer = new byte[expectedFileSize];
            ioctx.read(oid, expectedFileSize, 0, readBuffer);
            for (int i = 0; i < buffer.length; i++) {
                assertEquals(buffer[i], readBuffer[i]);
            }
            for (int i = 0; i < buffer.length / 2; i++) {
                assertEquals(buffer[i], readBuffer[i + buffer.length]);
            }
        } finally {
            cleanupObject(rados, ioctx, oid);
        }
    }

    /**
     * Use IOContext.writeFull to create a new object, than again writeFull with less data and verify
     * that the file was truncated.
     */
    @Test
    public void testIoCtxWriteFull() throws Exception {
        /**
         * The object we will write to with the data
         */
        String oid = "rados-java_writeFull";
        byte[] content = "junit wrote this".getBytes();

        try {
            ioctx.writeFull(oid, content, content.length);

            String[] objects = ioctx.listObjects();
            assertTrue("We expect at least one object in the POOL", objects.length > 0);

            verifyDocument(oid, content);

            // only write the first 4 bytes
            ioctx.writeFull(oid, content, 4);
            assertEquals("The size doesn't match after the smaller writeFull", 4, ioctx.stat(oid).getSize());

            verifyDocument(oid, Arrays.copyOf(content, 4));
        } finally {
            cleanupObject(rados, ioctx, oid);
        }
    }

    @Test
    public void testIoCtxXAttrOp() throws RadosException {
        String oid = "rados-java_xattr";
        String key = "xattr";
        String value = "bala";
        byte[] content = "junit wrote this".getBytes();

        try {
            // object must be exists, so we can read/write xattr
            ioctx.writeFull(oid, content, content.length);

            ioctx.setXAttr(oid, key, value);
            assertEquals(ioctx.getXAttr(oid, key), value);
            ioctx.rmXAttr(oid, key);
        } finally {
            cleanupObject(rados, ioctx, oid);
        }
    }

    private void verifyDocument(String oid, byte[] content) throws RadosException {
        byte[] buf = new byte[content.length];
        int len = ioctx.read(oid, content.length, 0, buf);
        assertEquals(len, content.length);
        RadosObjectInfo info = ioctx.stat(oid);

        assertEquals("The object names didn't match", oid, info.getOid());
        assertEquals("The size of what we wrote doesn't match with the stat", content.length, info.getSize());
        assertTrue("The content we read was different from what we wrote", Arrays.equals(content, buf));

        long now = System.currentTimeMillis() / 1000;
        assertFalse("The mtime was in the future", now < info.getMtime());
    }

    private void cleanupObject(Rados r, IoCTX io, String oid) throws RadosException {
        if (r != null) {
            if (io != null) {
                io.remove(oid);
            }
        }
    }

    @Test
    public void testIoCtxPoolStat() throws Exception {
        RadosPoolInfo info = ioctx.poolStat();
        assertTrue(info.num_objects >= 0); // rem: write an object first to make sure!
    }

    @Test
    public void testIoCtxSnapshot() throws Exception {
        String snapname = "my-new-snapshot";

        ioctx.snapCreate(snapname);

        long snapid = ioctx.snapLookup(snapname);
        long time = ioctx.snapGetStamp(snapid);
        String snapnamebuf = ioctx.snapGetName(snapid);

        Long[] snaps = ioctx.snapList();

        ioctx.snapRemove(snapname);

        assertTrue("There should at least be one snapshot", snaps.length >= 1);
        assertEquals("The snapshot names didn't match", snapname, snapnamebuf);

        long now = System.currentTimeMillis() / 1000;
            /* Add 5 seconds to deal with clock differences */
        assertTrue("The timestamp was in the future. Clocks synced?", (now + 5) >= time);
    }

    public void testReadRanges() {
        try {
            final String oid = "foobar.txt";
            final String content = "The quick brown fox jumped over the lazy dog.";
            ioctx.write(oid, content);
            final ReadOp rop = ioctx.readOpCreate();
            final Map<ReadResult,String> data = new HashMap<>();
            data.put(rop.queueRead(0, 3), content.substring(0, 0+3)/*The*/);
            data.put(rop.queueRead(20, 6), content.substring(20,20+6)/*jumped*/);
            data.put(rop.queueRead(10, 5), content.substring(10,10+5)/*brown*/);
            rop.operate(oid, 0);
            for ( Map.Entry<ReadResult,String> e : data.entrySet() ) {
            	byte[] buf = new byte[(int)e.getKey().getBytesRead()];
            	e.getKey().getBuffer().get(buf);
            	assertEquals(e.getValue(), new String(buf,java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
        catch ( Exception e ) {
            fail(e.getMessage());
        }
    }

    public void testListPartial() {
        /**
         * The object we will write to with the data
         */
        Rados r = null;
        IoCTX io = null;
        String oid = "rados-java_item_";
        String content = "junit wrote this ";
        int nb = 100;

        try {
            System.out.println("Start");
            for (int i = 0; i < nb; i++) {
                byte []bytes = (content + i).getBytes();
                ioctx.writeFull(oid+i, bytes, bytes.length);
            }

            String [] allOids = ioctx.listObjects();
            assertTrue("Global number of items should be " + nb, allOids.length == nb);

            // Check reading all items in 10 parts
            ListCtx listCtx = ioctx.listObjectsPartial(nb/10);
            assertTrue("We expect the list to have right now a size of 0", listCtx.size() == 0);
            int totalRead = 0;
            int subnb = 0;
            for (int i = 0; i < 10; i++) {
                subnb = listCtx.nextObjects();
                totalRead += subnb;
                assertTrue("We expect the list to have right now a size of " + (nb / 10), listCtx.size() == nb / 10);
                assertTrue("We expect to have a correct oid", listCtx.getObjects()[0].startsWith(oid));
                String []oids = listCtx.getObjects();
                assertTrue("We expect the subset to have right now a size of " + (nb / 10), oids.length == nb / 10);
            }
            subnb = listCtx.nextObjects();
            assertTrue("We expect the list to have right now a size of " + 0, listCtx.size() == 0);
            totalRead += subnb;
            assertTrue("We expect the number of read items to be " + nb, totalRead == nb);
            listCtx.close();

            // Check reading half items in 5 parts (other half being ignored)
            listCtx = ioctx.listObjectsPartial(nb/10);
            assertTrue("We expect the list to have right now a size of 0", listCtx.size() == 0);
            totalRead = 0;
            for (int i = 0; i < 5; i++) {
                subnb = listCtx.nextObjects(nb / 10);
                totalRead += subnb + (nb / 10);
                assertTrue("We expect the list to have right now a size of " + (nb / 10), listCtx.size() == nb / 10);
                assertTrue("We expect to have a correct oid", listCtx.getObjects()[0].startsWith(oid));
                String []oids = listCtx.getObjects();
                assertTrue("We expect the subset to have right now a size of " + (nb / 10), oids.length == nb / 10);
            }
            subnb = listCtx.nextObjects();
            assertTrue("We expect the list to have right now a size of " + 0, listCtx.size() == 0);
            totalRead += subnb;
            assertTrue("We expect the number of read items to be " + nb, totalRead == nb);
            listCtx.close();

            // Check reading some items then close and then check listCtx is empty
            listCtx = ioctx.listObjectsPartial(nb/10);
            assertTrue("We expect the list to have right now a size of 0", listCtx.size() == 0);
            totalRead = 0;
            subnb = listCtx.nextObjects(nb / 10);
            assertTrue("We expect the list to have right now a size of " + (nb / 10), listCtx.size() == nb / 10);
            assertTrue("We expect to have a correct oid", listCtx.getObjects()[0].startsWith(oid));
            listCtx.close();
            subnb = listCtx.nextObjects();
            assertTrue("We expect the list to have right now a size of " + 0, listCtx.size() == 0);
        } catch (RadosException e) {
            fail(e.getMessage() + ": " + e.getReturnValue());
        }
        finally {
            try {
                if(r != null) {
                    if(ioctx != null) {
                        for (int i = 0; i < nb; i++) {
                            ioctx.remove(oid+i);
                        }
                    }
                }
            }
            catch (RadosException e) {
            }
        }
    }

    static class RadosFinalizeTest extends Rados {

        public RadosFinalizeTest(String id) {
            super(id);
            // System.err.println(String.format("Initialized with clusterptr: %x, %s", Pointer.nativeValue(this.clusterPtr), this.toString()));
        }

        @Override
        public void finalize() throws Throwable {
            assertTrue(Pointer.nativeValue(clusterPtr) > 0);
            // System.err.println(String.format("Finalizing with clusterptr: %x, %s", Pointer.nativeValue(this.clusterPtr), this.toString()));
            super.finalize();
        }
    }

    @Test
    public void testRadosFinalization() throws Exception {
        for (int i = 0; i < 10; i++) {
            RadosFinalizeTest r = new RadosFinalizeTest(ID);
            r.confReadFile(new File(CONFIG_FILE));
            r.connect();
            r = null;
            System.gc();
            System.runFinalization();
        }
    }
}
