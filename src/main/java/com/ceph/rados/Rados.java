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

import com.ceph.rados.exceptions.RadosException;
import com.ceph.rados.jna.RadosClusterInfo;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.Pointer;
import com.sun.jna.Memory;
import com.sun.jna.Native;

import java.io.File;
import java.util.concurrent.Callable;

import static com.ceph.rados.Library.rados;

public class Rados extends RadosBase {

    protected Pointer clusterPtr;
    private boolean connected;

    /**
     * Construct a RADOS Object which invokes rados_create
     *
     * @param id
     *            the cephx id to authenticate with
     */
    public Rados(String id) {
        PointerByReference clusterPtr = new PointerByReference();
        rados.rados_create(clusterPtr, id);
        this.clusterPtr = clusterPtr.getValue();
    }


	/**
	 * Construct a RADOS Object which invokes rados_create2
	 *
	 * @param clustername The name of the cluster (usually "ceph").
	 * @param name The name of the user (e.g., client.admin, client.user)
	 * @param flags Flag options (future use).
	 */

	public Rados (String clustername, String name, long flags) {
		PointerByReference clusterPtr = new PointerByReference();
		rados.rados_create2(clusterPtr, clustername, name, flags);
		this.clusterPtr = clusterPtr.getValue();
	}


    /**
     * Construct a RADOS Object which invokes rados_create
     */
    public Rados() {
        this(null);
    }

    /**
     * Some methods should not be called when not connected
     * or vise versa
     */
    private void verifyConnected(boolean required) throws RadosException {
        if (required && !this.connected) {
            throw new RadosException("This method should not be called in a disconnected state.");
        }
        if (!required && this.connected) {
             throw new RadosException("This method should not be called in a connected state.");
        }
    }

    /**
     * Read a Ceph configuration file
     *
     * @param file
     *            A file object with the path to a ceph.conf
     * @throws RadosException
     */
    public void confReadFile(final File file) throws RadosException {
        this.verifyConnected(false);
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_conf_read_file(clusterPtr, file.getAbsolutePath());
            }
        }, "Failed reading configuration file %s", file.getAbsolutePath());
    }

    /**
     * Set a RADOS configuration option
     *
     * @param option
     *            the name of the option
     * @param value
     *            the value configuration value
     * @throws RadosException
     */
    public void confSet(final String option, final String value) throws RadosException {
        this.verifyConnected(false);
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_conf_set(clusterPtr, option, value);
            }
        }, "Could not set configuration option %s", option);
    }

    /**
     * Retrieve a RADOS configuration option's value
     *
     * @param option
     *            the name of the option
     * @return
     *            the value of the option
     * @throws RadosException
     */
    public String confGet(final String option) throws RadosException {
        final byte[] buf = new byte[256];
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_conf_get(clusterPtr, option, buf, buf.length);
            }
        }, "Unable to retrieve the value of configuration option %s", option);
        return Native.toString(buf);
    }

    /**
     * Connect to the Ceph cluster
     *
     * @throws RadosException
     */
    public void connect() throws RadosException {
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_connect(clusterPtr);
            }
        }, "Failed to connect to the Ceph cluster");
        this.connected = true;
    }

    /**
     * Get the cluster's fsid
     *
     * @return
     *        A string containing the cluster's fsid
     * @throws RadosException
     */
    public String clusterFsid() throws RadosException {
        this.verifyConnected(true);
        final byte[] buf = new byte[256];
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_cluster_fsid(clusterPtr, buf, buf.length);
            }
        }, "Failed to retrieve the cluster's fsid");
        return Native.toString(buf);
    }

    /**
     * Get the cluster stats
     *
     * @return RadosClusterInfo
     * @throws RadosException
     */
    public RadosClusterInfo clusterStat() throws RadosException {
        this.verifyConnected(true);
        final RadosClusterInfo result = new RadosClusterInfo();
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_cluster_stat(clusterPtr, result);
            }
        }, "Failed to retrieve cluster's status");
        return result;
    }

    /**
     * Create a RADOS pool
     *
     * @param name
     *            the name of the pool to be created
     * @throws RadosException
     */
    public void poolCreate(final String name) throws RadosException {
        this.verifyConnected(true);
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_pool_create(clusterPtr, name);
            }
        }, "Failed to create pool %s", name);
    }

    /**
     * Create a RADOS pool and set a auid
     *
     * @param name
     *            the name of the pool to be created
     * @param auid
     *            the owner ID for the new pool
     * @throws RadosException
     */
    public void poolCreate(final String name, final long auid) throws RadosException {
        this.verifyConnected(true);
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_pool_create_with_auid(clusterPtr, name, auid);
            }
        }, "Failed to create pool %s with auid %s", name, auid);
    }

    /**
     * Create a RADOS pool and set a auid and crushrule
     *
     * @param name
     *            the name of the pool to be created
     * @param auid
     *            the owner ID for the new pool
     * @param crushrule
     *            the crushrule for this pool
     * @throws RadosException
     */
    public void poolCreate(final String name, final long auid, final long crushrule) throws RadosException {
        this.verifyConnected(true);
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_pool_create_with_all(clusterPtr, name, auid, crushrule);
            }
        }, "Failed to create pool %s with auid %s and crushrule %s", name, auid, crushrule);
    }

    /**
     * Delete a RADOS pool
     *
     * @param name
     *            the name of the pool to be deleted
     * @throws RadosException
     */
    public void poolDelete(final String name) throws RadosException {
        this.verifyConnected(true);
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_pool_delete(clusterPtr, name);
            }
        }, "Failed to delete pool %s", name);
    }

    /**
     * List all the RADOS pools
     *
     * @return String[] list of pools
     * @throws RadosException
     */
    public String[] poolList() throws RadosException {
        this.verifyConnected(true);
        byte[] temp_buf = new byte[0];
        int len = rados.rados_pool_list(this.clusterPtr, temp_buf, temp_buf.length);
        final byte[] buf = getPoolList(len);
        return new String(buf).split("\0");
    }

    private byte[] getPoolList(int len) throws RadosException {
        final byte[] buf = new byte[len];
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_pool_list(clusterPtr, buf, buf.length);
            }
        }, "Failed to retrieve list of pools");
        return buf;
    }

    /**
     * Get the ID of a RADOS pool
     *
     * @param name
     *           The name of the pool
     * @return long
     * @throws RadosException
     */
    public long poolLookup(final String name) throws RadosException {
        return handleReturnCode(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return rados.rados_pool_lookup(Rados.this.clusterPtr, name);
            }
        }, "Failed to retrieve id of the pool");
    }

    /**
     * Get the name of a RADOS pool
     *
     * @param id
     *           The id of the pool
     * @return String
     * @throws RadosException
     */
    public String poolReverseLookup(final long id) throws RadosException {
        final byte[] buf = new byte[512];
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_pool_reverse_lookup(clusterPtr, id, buf, buf.length);
            }
        }, "Failed to fetch name of the pool");
        return new String(buf).trim();
    }

    /**
     * Create a IoCTX
     *
     * @param pool
     *           The name of the RADOS pool
     * @return IoCTX
     * @throws RadosException
     */
    public IoCTX ioCtxCreate(final String pool) throws RadosException {
        final Pointer p = new Memory(Pointer.SIZE);
        handleReturnCode(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return rados.rados_ioctx_create(clusterPtr, pool, p);
            }
        }, "Failed to create the IoCTX for pool %s", pool);
        return new IoCTX(p);
    }

    /**
     * Destroy a IoCTX
     *
     * @param io
     *             A IoCTX object
     */
    public void ioCtxDestroy(IoCTX io) {
        rados.rados_ioctx_destroy(io.getPointer());
    }


    /**
     * Get the global unique ID of the current connection
     *
     * @return long
     */
    public long getInstanceId() throws RadosException {
        this.verifyConnected(true);
        return rados.rados_get_instance_id(this.clusterPtr);
    }

    /**
     * Get the librados version
     *
     * @return a int array with the minor, major and extra version
     */
    public static int[] getVersion() {
        IntByReference minor = new IntByReference();
        IntByReference major = new IntByReference();
        IntByReference extra = new IntByReference();
        rados.rados_version(minor, major, extra);
        return new int[]{minor.getValue(), major.getValue(), extra.getValue()};
    }

    /**
     * Shuts rados down
     */
    public void shutDown() {
        if (this.clusterPtr != null) {
            rados.rados_shutdown(this.clusterPtr);
            this.clusterPtr = null;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        shutDown();
        super.finalize();
    }

}
