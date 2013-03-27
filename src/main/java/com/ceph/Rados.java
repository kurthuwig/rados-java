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

import com.sun.jna.ptr.IntByReference;
import com.sun.jna.Pointer;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import java.io.File;

import static com.ceph.Library.rados;

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
        this.clusterPtr = new Memory(Pointer.SIZE);
        rados.rados_create(this.clusterPtr, id);
    }

    /**
     * Construct a RADOS Object which invokes rados_create
     *
     */
    public Rados() {
        this.clusterPtr = new Memory(Pointer.SIZE);
        rados.rados_create(this.clusterPtr, "");
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
        int r = rados.rados_conf_read_file(this.clusterPtr.getPointer(0), file.getAbsolutePath());
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
        int r = rados.rados_conf_set(this.clusterPtr.getPointer(0), option, value);
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
        this.verifyConnected(false);
        byte[] buf = new byte[256];
        int r = rados.rados_conf_get(this.clusterPtr.getPointer(0), option, buf, buf.length);
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
        int r = rados.rados_connect(this.clusterPtr.getPointer(0));
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
        int r = rados.rados_cluster_fsid(this.clusterPtr.getPointer(0), buf, buf.length);
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
        int r = rados.rados_cluster_stat(this.clusterPtr.getPointer(0), result);
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
        rados.rados_pool_create(this.clusterPtr.getPointer(0), name);
    }

    public void poolCreate(String name, long auid) throws RadosException {
        this.verifyConnected(true);
        rados.rados_pool_create_with_auid(this.clusterPtr.getPointer(0), name, auid);
    }

    public void poolCreate(String name, long auid, long crushrule) throws RadosException {
        this.verifyConnected(true);
        rados.rados_pool_create_with_all(this.clusterPtr.getPointer(0), name, auid, crushrule);
    }

    protected void finalize() throws Throwable {
        rados.rados_shutdown(this.clusterPtr.getPointer(0));
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
        int len = rados.rados_pool_list(this.clusterPtr.getPointer(0), temp_buf, temp_buf.length);

        byte[] buf = new byte[len];
        int r = rados.rados_pool_list(this.clusterPtr.getPointer(0), buf, buf.length);
        if (r < 0) {
            throw new RadosException("Couldn't list all pools", r);
        }
        return new String(buf).split("\0");
    }

    /**
     * Create a IoCTX
     *
     * @param name
     *           The name of the RADOS pool
     * @return IoCTX
     * @throws RadosException
     */
    public IoCTX ioCtxCreate(String pool) throws RadosException {
        Pointer p = new Memory(Pointer.SIZE);
        int r = rados.rados_ioctx_create(this.clusterPtr.getPointer(0), pool, p);
        if (r < 0) {
            throw new RadosException("Failed to create the IoCTX for " + pool, r);
        }

        return new IoCTX(p);
    }

    /**
     * Destroy a IoCTX
     *
     * @param ioctx
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
        return rados.rados_get_instance_id(this.clusterPtr.getPointer(0));
    }

    /**
     * Get the librados version
     *
     * @return a int array with the minor, major and extra version
     */
    public int[] getVersion() {
        IntByReference minor = new IntByReference();
        IntByReference major = new IntByReference();
        IntByReference extra = new IntByReference();
        rados.rados_version(minor, major, extra);
        int[] returnValue = {minor.getValue(), major.getValue(), extra.getValue()};
        return returnValue;
    }

}