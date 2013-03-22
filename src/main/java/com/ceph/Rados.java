package com.ceph;

import com.sun.jna.ptr.IntByReference;
import com.sun.jna.Pointer;
import com.sun.jna.Memory;
import java.io.File;

import static com.ceph.Library.rados;

public class Rados {

    protected Pointer clusterPtr;

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
     * Read a Ceph configuration file
     *
     * @param file
     *            A file object with the path to a ceph.conf
     * @throws RadosException
     */
    public void confReadFile(File file) throws RadosException {
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
        int r = rados.rados_conf_set(this.clusterPtr.getPointer(0), option, value);
        if (r < 0) {
            throw new RadosException("Could not set configuration option " + option, r);
        }
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
    }

    /**
     * Create a RADOS pool
     *
     * @param name
     *            the name of the pool to be created
     * @throws RadosException
     */
    public void poolCreate(String name) {
        rados.rados_pool_create(this.clusterPtr.getPointer(0), name);
    }

    protected void finalize() throws Throwable {
        rados.rados_shutdown(this.clusterPtr.getPointer(0));
        super.finalize();
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