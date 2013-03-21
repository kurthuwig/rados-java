package com.ceph.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.ptr.IntByReference;

public interface Rados extends Library {

    Rados INSTANCE = (Rados) Native.loadLibrary("rados", Rados.class);

    void rados_version(IntByReference major, IntByReference minor, IntByReference extra);
    int rados_create(ClusterPointer cluster, String id);
    int rados_conf_read_file(ClusterPointer cluster, String path);
    int rados_conf_set(ClusterPointer cluster, String option, String value);
    int rados_conf_get(ClusterPointer cluster, String option, String value);
    int radus_shutdown(ClusterPointer cluster);

}