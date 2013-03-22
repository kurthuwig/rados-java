package com.ceph;

import com.sun.jna.ptr.IntByReference;
import com.sun.jna.Pointer;
import com.sun.jna.Memory;
import java.io.File;

import static com.ceph.Library.rados;

public class Rados {

    protected Pointer clusterPtr;

    public Rados(String id) {
        this.clusterPtr = new Memory(Pointer.SIZE);
        rados.rados_create(this.clusterPtr, id);
    }

    public Rados() {
        this.clusterPtr = new Memory(Pointer.SIZE);
        rados.rados_create(this.clusterPtr, "");
    }

    public void confReadFile(File file) throws RadosException {
        int r = rados.rados_conf_read_file(this.clusterPtr.getPointer(0), file.getAbsolutePath());
        if (r < 0) {
            throw new RadosException("Failed reading configuration file " + file.getAbsolutePath());
        }
    }

    public void confSet(String option, String value) throws RadosException {
        int r = rados.rados_conf_set(this.clusterPtr.getPointer(0), option, value);
        if (r < 0) {
            throw new RadosException("Could not set configuration option " + option);
        }
    }

    public void connect() throws RadosException {
        int r = rados.rados_connect(this.clusterPtr.getPointer(0));
        if (r < 0) {
            throw new RadosException("The connection to the Ceph cluster failed");
        }
    }

    public void poolCreate(String name) {
        rados.rados_pool_create(this.clusterPtr.getPointer(0), name);
    }

    protected void finalize() throws Throwable {
        rados.rados_shutdown(this.clusterPtr.getPointer(0));
        super.finalize();
    }

    public int[] getVersion() {
        IntByReference minor = new IntByReference();
        IntByReference major = new IntByReference();
        IntByReference extra = new IntByReference();
        rados.rados_version(minor, major, extra);
        int[] returnValue = {minor.getValue(), major.getValue(), extra.getValue()};
        return returnValue;
    }

}