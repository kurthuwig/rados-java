/*
 * RADOS Java - Java bindings for librados
 *
 * Copyright (C) 2013 Wido den Hollander <wido@42on.com>
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

import com.ceph.rados.jna.RadosClusterInfo;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.Pointer;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import java.io.File;

import static com.ceph.rados.Library.rados;

public class Rados {

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
     *
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
    public void confReadFile(File file) throws RadosException {
        this.verifyConnected(false);
        int r = rados.rados_conf_read_file(this.clusterPtr, file.getAbsolutePath());
        if (r < 0) {
            throw new RadosException("Failed reading configuration file " + file.getAbsolutePath(), r);
        }
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
    public void confSet(String option, String value) throws RadosException {
        this.verifyConnected(false);
        int r = rados.rados_conf_set(this.clusterPtr, option, value);
        if (r < 0) {
            throw new RadosException("Could not set configuration option " + option, r);
        }
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
    public String confGet(String option) throws RadosException {
        byte[] buf = new byte[256];
        int r = rados.rados_conf_get(this.clusterPtr, option, buf, buf.length);
        if (r < 0) {
            throw new RadosException("Unable to retrieve the value of configuration option " + option, r);
        }
        return Native.toString(buf);
    }

    /**
     * Connect to the Ceph cluster
     *
     * @throws RadosException
     */
    public void connect() throws RadosException {
        int r = rados.rados_connect(this.clusterPtr);
        if (r < 0) {
            throw new RadosException("The connection to the Ceph cluster failed", r);
        }
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
        byte[] buf = new byte[256];
        int r = rados.rados_cluster_fsid(this.clusterPtr, buf, buf.length);
        if (r < 0) {
            throw new RadosException("Unable to retrieve the cluster's fsid", r);
        }
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
        RadosClusterInfo result = new RadosClusterInfo();
        int r = rados.rados_cluster_stat(this.clusterPtr, result);
        return result;
    }

    /**
     * Create a RADOS pool
     *
     * @param name
     *            the name of the pool to be created
     * @throws RadosException
     */
    public void poolCreate(String name) throws RadosException {
        this.verifyConnected(true);
        int r = rados.rados_pool_create(this.clusterPtr, name);
        if (r < 0) {
            throw new RadosException("Failed to create pool " + name, r);
        }
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
    public void poolCreate(String name, long auid) throws RadosException {
        this.verifyConnected(true);
        int r = rados.rados_pool_create_with_auid(this.clusterPtr, name, auid);
        if (r < 0) {
            throw new RadosException("Failed to create pool " + name + " with auid " + auid, r);
        }
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
    public void poolCreate(String name, long auid, long crushrule) throws RadosException {
        this.verifyConnected(true);
        int r = rados.rados_pool_create_with_all(this.clusterPtr, name, auid, crushrule);
        if (r < 0) {
            throw new RadosException("Failed to create pool " + name + " with auid " + auid +
                                     " and crushrule " + crushrule, r);
        }
    }

    /**
     * Delete a RADOS pool
     *
     * @param name
     *            the name of the pool to be deleted
     * @throws RadosException
     */
    public void poolDelete(String name) throws RadosException {
        this.verifyConnected(true);
        int r = rados.rados_pool_delete(this.clusterPtr, name);
        if (r < 0) {
            throw new RadosException("Failed to delete pool " + name, r);
        }
    }

    protected void finalize() throws Throwable {
        rados.rados_shutdown(this.clusterPtr);
        super.finalize();
    }

    /**
     * List all the RADOS pools
     *
     * @return String[]
     * @throws RadosException
     */
    public String[] poolList() throws RadosException {
        this.verifyConnected(true);
        byte[] temp_buf = new byte[256];
        int len = rados.rados_pool_list(this.clusterPtr, temp_buf, temp_buf.length);

        byte[] buf = new byte[len];
        int r = rados.rados_pool_list(this.clusterPtr, buf, buf.length);
        if (r < 0) {
            throw new RadosException("Couldn't list all pools", r);
        }
        return new String(buf).split("\0");
    }

    /**
     * Get the ID of a RADOS pool
     *
     * @param name
     *           The name of the pool
     * @return long
     * @throws RadosException
     */
    public long poolLookup(String name) throws RadosException {
        long r = rados.rados_pool_lookup(this.clusterPtr, name);
        if (r < 0) {
            throw new RadosException("Couldn't fetch the ID of the pool. Does it exist?", (int)r);
        }
        return r;
    }

    /**
     * Get the name of a RADOS pool
     *
     * @param id
     *           The id of the pool
     * @return String
     * @throws RadosException
     */
    public String poolReverseLookup(long id) throws RadosException {
        byte[] buf = new byte[512];
        int r = rados.rados_pool_reverse_lookup(this.clusterPtr, id, buf, buf.length);
        if (r < 0) {
            throw new RadosException("Couldn't fetch the name of the pool. Does it exist?", r);
        }
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
    public IoCTX ioCtxCreate(String pool) throws RadosException {
        Pointer p = new Memory(Pointer.SIZE);
        int r = rados.rados_ioctx_create(this.clusterPtr, pool, p);
        if (r < 0) {
            throw new RadosException("Failed to create the IoCTX for " + pool, r);
        }

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
        int[] returnValue = {minor.getValue(), major.getValue(), extra.getValue()};
        return returnValue;
    }

}
