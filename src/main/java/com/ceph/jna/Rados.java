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

package com.ceph.jna;

import com.ceph.RadosClusterInfo;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.LongByReference;
import com.sun.jna.Pointer;

public interface Rados extends Library {

    Rados INSTANCE = (Rados) Native.loadLibrary("rados", Rados.class);

    void rados_version(IntByReference major, IntByReference minor, IntByReference extra);
    int rados_create(Pointer cluster, String id);
    int rados_conf_read_file(Pointer cluster, String path);
    int rados_conf_set(Pointer cluster, String option, String value);
    int rados_conf_get(Pointer cluster, String option, byte[] buf, int len);
    int rados_cluster_fsid(Pointer cluster, byte[] buf, int len);
    int rados_cluster_stat(Pointer cluster, RadosClusterInfo result);
    int rados_connect(Pointer cluster);
    int rados_pool_create(Pointer cluster, String name);
    int rados_pool_create_with_auid(Pointer cluster, String name, long auid);
    int rados_pool_create_with_all(Pointer cluster, String name, long auid, long crushrule);
    int rados_pool_create_with_crush_rule(Pointer cluster, String name, long crushrule);
    int rados_pool_list(Pointer cluster, byte[] buf, int len);
    long rados_pool_lookup(Pointer cluster, String name);
    long rados_get_instance_id(Pointer cluster);
    int rados_ioctx_create(Pointer cluster, String pool, Pointer ioctx);
    void rados_ioctx_destroy(Pointer ioctx);
    long rados_ioctx_get_id(Pointer ioctx);
    int rados_ioctx_pool_set_auid(Pointer ioctx, long auid);
    int rados_ioctx_pool_get_auid(Pointer ioctx, LongByReference auid);
    int rados_ioctx_get_pool_name(Pointer ioctx, byte[] buf, int len);
    void rados_ioctx_locator_set_key(Pointer ioctx, String key);
    int rados_shutdown(Pointer cluster);

}