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

public class RadosObjectInfo {

    private String oid;
    private long size;
    private long mtime;

    public RadosObjectInfo(String oid, long size, long mtime) {
        this.oid = oid;
        this.size = size;
        this.mtime = mtime;
    }

    /**
     * Return the object name
     * @return String
    */
    public String getOid() {
        return this.oid;
    }

    /**
     * Returns the size in bytes
     * @return long
     */
    public long getSize() {
        return this.size;
    }

    /**
     * Returns the modification time
     * @return long
     */
    public long getMtime() {
        return this.mtime;
    }

}
