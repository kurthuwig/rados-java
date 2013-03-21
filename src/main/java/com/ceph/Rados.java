package com.ceph;

import com.sun.jna.ptr.IntByReference;
import com.ceph.jna.ClusterPointer;

import static com.ceph.Library.rados;

public class Rados {

    private String id;

    protected ClusterPointer clusterPtr;

    public Rados() {
        this.id = null;
    }

    public Rados(String id) {
        this.id = id;
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