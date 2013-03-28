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

import com.ceph.jna.RadosObjectInfo;
import com.sun.jna.Pointer;
import com.sun.jna.Native;
import com.sun.jna.Memory;
import com.sun.jna.ptr.LongByReference;
import java.util.ArrayList;
import java.util.List;

import static com.ceph.Library.rados;

public class IoCTX {

    private Pointer ioCtxPtr;

    /**
     * Create a new IO Context object
     *
     * This constructor should never be called, IO Context
     * objects are created by the RADOS class and returned
     * when creating a IO Context there
    */
    public IoCTX(Pointer p) {
        this.ioCtxPtr = p;
    }

    /**
     * Return the pointer to the IO Context
     *
     * This method is used internally and by the RADOS class
     * to destroy a IO Context
     *
     * @return Pointer
     */
    public Pointer getPointer() {
        return this.ioCtxPtr.getPointer(0);
    }

    /**
     * Get the pool ID of this context
     *
     * @return long
     */
    public long getId() {
        return rados.rados_ioctx_get_id(this.getPointer());
    }

    /**
     * Set the associated auid owner of the current pool
     *
     * @param auid
     *           The new auid
     * @throws RadosException
     */
    public void setAuid(long auid) throws RadosException {
        int r = rados.rados_ioctx_pool_set_auid(this.getPointer(), auid);
        if (r < 0) {
            throw new RadosException("Failed to set the auid to " + auid, r);
        }
    }

    /**
     * Get the associated auid owner of the current pool
     *
     * @return long
     * @throws RadosException
     */
    public long getAuid() throws RadosException {
        LongByReference auid = new LongByReference();
        int r = rados.rados_ioctx_pool_get_auid(this.getPointer(), auid);
        if (r < 0) {
            throw new RadosException("Failed to get the auid", r);
        }
        return auid.getValue();
    }

    /**
     * Get the pool name of the context
     *
     * @return String
     * @throws RadosException
     */
    public String getPoolName() throws RadosException {
        byte[] buf = new byte[1024];
        int r = rados.rados_ioctx_get_pool_name(this.getPointer(), buf, buf.length);
        if (r < 0) {
            throw new RadosException("Failed to get the pool name", r);
        }
        return Native.toString(buf);
    }

    /**
     * Set the locator key
     *
     * @param key
     *          The new locator key or NULL to remove a previous one
     */
    public void locatorSetKey(String key) {
        rados.rados_ioctx_locator_set_key(this.getPointer(), key);
    }

    /**
     * List all objects in a pool
     *
     * @return String[]
     * @throws RadosException
     */
    public String[] listObjects() throws RadosException {
        Pointer entry = new Memory(Pointer.SIZE);
        List<String> objects = new ArrayList<String>();
        Pointer list = new Memory(Pointer.SIZE);

        int r = rados.rados_objects_list_open(this.getPointer(), list);
        if (r < 0) {
            throw new RadosException("Failed listing all objects", r);
        }

        while (rados.rados_objects_list_next(list.getPointer(0), entry, null) == 0) {
            objects.add(entry.getPointer(0).getString(0));
        }

        rados.rados_objects_list_close(list.getPointer(0));

        return objects.toArray(new String[objects.size()]);
    }

    /**
     * Write to an object
     *
     * @param oid
     *          The object to write to
     * @param buf
     *          The content to write
     * @param offset
     *          The offset when writing
     * @throws RadosException
     */
    public void write(String oid, String buf, long offset) throws RadosException {
        int r = rados.rados_write(this.getPointer(), oid, buf, buf.length(), offset);
        if (r < 0) {
            throw new RadosException("Failed writing " + buf.length() + " bytes with offset " + offset + " to " + oid, r);
        }
    }

    /**
     * Write to an object without an offset
     *
     * @param oid
     *          The object to write to
     * @param buf
     *          The content to write
     * @throws RadosException
     */
    public void write(String oid, String buf) throws RadosException {
        int r = rados.rados_write_full(this.getPointer(), oid, buf, buf.length());
        if (r < 0) {
            throw new RadosException("Failed writing " + buf.length() + " bytes to " + oid, r);
        }
    }

    /**
     * Remove an object
     *
     * @param oid
     *          The object to remove
     * @throws RadosException
     */
    public void remove(String oid) throws RadosException {
        int r = rados.rados_remove(this.getPointer(), oid);
        if (r < 0) {
            throw new RadosException("Failed removing " + oid, r);
        }
    }

    /**
     * Read data from an object
     *
     * @param oid
     *          The object's name
     * @param length
     *          Amount of bytes to read
     * @param offset
     *          The offset where to start reading
     * @throws RadosException
     */
    public String read(String oid, int length, long offset) throws RadosException {
        byte[] buf = new byte[length];
        int r = rados.rados_read(this.getPointer(), oid, buf, length, offset);
        if (r < 0) {
            throw new RadosException("Failed reading " + length + " bytes with offset " + offset + " from " + oid, r);
        }
        return new String(buf);
    }

    /**
     * Resize an object
     *
     * @param oid
     *           The object to resize
     * @param size
     *          The new length of the object.  If this enlarges the object,
     *          the new area is logically filled with
     *          zeroes. If this shrinks the object, the excess data is removed.
     * @throws RadosException
     */
    public void truncate(String oid, long size) throws RadosException {
        int r = rados.rados_trunc(this.getPointer(), oid, size);
        if (r < 0) {
            throw new RadosException("Failed resizing " + oid + " to " + size + " bytes", r);
        }
    }

    /**
     * Append data to an object
     *
     * @param oid
     *           The name to append to
     * @param buf
     *           The data to append
     * @throws RadosException
     */
    public void append(String oid, String buf) throws RadosException {
        int r = rados.rados_append(this.getPointer(), oid, buf, buf.length());
        if (r < 0) {
            throw new RadosException("Failed appending " + buf.length() + " bytes to " + oid, r);
        }
    }

    /**
     * Stat an object
     *
     * @param oid
     *          The name of the object
     * @return RadosObjectInfo
     *           The size and mtime of the object
     * @throws RadosException
     */
    public RadosObjectInfo stat(String oid) throws RadosException {
        LongByReference size = new LongByReference();
        LongByReference mtime = new LongByReference();
        int r = rados.rados_stat(this.getPointer(), oid, size, mtime);
        if (r < 0) {
            throw new RadosException("Failed performing a stat on " + oid, r);
        }
        return new RadosObjectInfo(oid, size.getValue(), mtime.getValue());
    }
}
