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

package com.ceph.rados.jna;

import com.ceph.rados.jna.RadosClusterInfo;
import com.ceph.rados.jna.RadosPoolInfo;
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
    int rados_pool_reverse_lookup(Pointer cluster, long id, byte[] buf, long len);
    int rados_ioctx_pool_stat(Pointer ioctx, RadosPoolInfo result);
    long rados_get_instance_id(Pointer cluster);
    int rados_ioctx_create(Pointer cluster, String pool, Pointer ioctx);
    void rados_ioctx_destroy(Pointer ioctx);
    long rados_ioctx_get_id(Pointer ioctx);
    int rados_ioctx_pool_set_auid(Pointer ioctx, long auid);
    int rados_ioctx_pool_get_auid(Pointer ioctx, LongByReference auid);
    int rados_ioctx_get_pool_name(Pointer ioctx, byte[] buf, int len);
    void rados_ioctx_locator_set_key(Pointer ioctx, String key);
    int rados_ioctx_snap_create(Pointer ioctx, String snapname);
    int rados_ioctx_snap_remove(Pointer ioctx, String snapname);
    int rados_ioctx_snap_lookup(Pointer ioctx, String snapname, LongByReference id);
    int rados_ioctx_snap_get_name(Pointer ioctx, long id, byte[] buf, long len);
    int rados_ioctx_snap_get_stamp(Pointer ioctx, long id, LongByReference time);
    int rados_ioctx_snap_list(Pointer ioctx, byte[] buf, int len);
    int rados_objects_list_open(Pointer ioctx, Pointer list);
    int rados_objects_list_next(Pointer list, Pointer entry, byte[] key);
    void rados_objects_list_close(Pointer list);
    int rados_write(Pointer ioctx, String oid, String buf, long len, long off);
    int rados_write_full(Pointer ioctx, String oid, String buf, long len);
    int rados_append(Pointer ioctx, String oid, String buf, long len);
    int rados_read(Pointer ioctx, String oid, byte[] buf, long len, long off);
    int rados_remove(Pointer ioctx, String oid);
    int rados_trunc(Pointer ioctx, String oid, long size);
    int rados_clone_range(Pointer ioctx, String dst, long dst_off, String src, long src_off, long len);
    int rados_stat(Pointer ioctxo, String oi, LongByReference size, LongByReference mtime);
    int rados_shutdown(Pointer cluster);

}
