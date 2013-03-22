package com.ceph.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.Pointer;

public interface Rados extends Library {

    Rados INSTANCE = (Rados) Native.loadLibrary("rados", Rados.class);

    void rados_version(IntByReference major, IntByReference minor, IntByReference extra);
    int rados_create(Pointer cluster, String id);
    int rados_conf_read_file(Pointer cluster, String path);
    int rados_conf_set(Pointer cluster, String option, String value);
    int rados_conf_get(Pointer cluster, String option, String value);
    int rados_connect(Pointer cluster);
    int rados_pool_create(Pointer cluster, String name);
    int rados_pool_create_with_auid(Pointer cluster, String name, long auid);
    int rados_pool_create_with_all(Pointer cluster, String name, long auid, long crushrule);
    int rados_pool_create_with_crush_rule(Pointer cluster, String name, long crushrule);
    int rados_shutdown(Pointer cluster);

}