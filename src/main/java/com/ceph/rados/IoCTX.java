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

package com.ceph.rados;

import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.jna.RadosPoolInfo;
import com.sun.jna.Pointer;
import com.sun.jna.Native;
import com.sun.jna.Memory;
import com.sun.jna.ptr.LongByReference;
import java.util.ArrayList;
import java.util.List;
import java.lang.IllegalArgumentException;

import static com.ceph.rados.Library.rados;

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
    public void write(String oid, String buf, long offset) throws RadosException, IllegalArgumentException {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset shouldn't be a negative value");
        }
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
    public String read(String oid, int length, long offset) throws RadosException, IllegalArgumentException {
        if (length < 0) {
            throw new IllegalArgumentException("Length shouldn't be a negative value");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset shouldn't be a negative value");
        }
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
    public void truncate(String oid, long size) throws RadosException, IllegalArgumentException {
        if (size < 0) {
            throw new IllegalArgumentException("Size shouldn't be a negative value");
        }
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
    * Efficiently copy a portion of one object to another
    *
    * If the underlying filesystem on the OSD supports it, this will be a
    * copy-on-write clone.
    *
    * The src and dest objects must be in the same pg. To ensure this,
    * the io context should have a locator key set (see IoCTX.locatorSetKey()).
    *
    * @param dst
    *          The destination object
    * @param dst_off
    *          The offset at the destination object
    * @param src
    *          The source object
    * @param src_off
    *          The offset at the source object
    * @param len
    *          The amount of bytes to copy
    * @throws RadosException
    */
    public void clone(String dst, long dst_off, String src, long src_off, long len) throws RadosException {
        int r = rados.rados_clone_range(this.getPointer(), dst, dst_off, src, src_off, len);
        if (r < 0) {
            throw new RadosException("Failed to copy " + len + " bytes from " + src + " to " + dst, r);
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

    /**
     * Stat the currently open pool
     *
     * @return RadosPoolInfo
     * @throws RadosException
     */
    public RadosPoolInfo poolStat() throws RadosException {
        RadosPoolInfo result = new RadosPoolInfo();
        int r = rados.rados_ioctx_pool_stat(this.getPointer(), result);
        if (r < 0) {
            throw new RadosException("Failed retrieving the pool stats", r);
        }
        return result;
    }

    /**
     * Create a snapshot
     *
     * @param snapname
     *           The name of the snapshot
     * @throws RadosException
     */
    public void snapCreate(String snapname) throws RadosException {
        int r = rados.rados_ioctx_snap_create(this.getPointer(), snapname);
        if (r < 0) {
            throw new RadosException("Failed to create snapshot " + snapname, r);
        }
    }

    /**
     * Remove a snapshot
     *
     * @param snapname
     *           The name of the snapshot
     * @throws RadosException
     */
    public void snapRemove(String snapname) throws RadosException {
        int r = rados.rados_ioctx_snap_remove(this.getPointer(), snapname);
        if (r < 0) {
            throw new RadosException("Failed to remove snapshot " + snapname, r);
        }
    }

    /**
     * Get the ID of a snapshot
     *
     * @param snapname
     *            The name of the snapshot
     * @return long
     * @throws RadosException
     */
    public long snapLookup(String snapname) throws RadosException {
        LongByReference id = new LongByReference();
        int r = rados.rados_ioctx_snap_lookup(this.getPointer(), snapname, id);
        if (r < 0) {
            throw new RadosException("Failed to lookup the ID of snapshot " + snapname, r);
        }
        return id.getValue();
    }

    /**
     * Get the name of a snapshot by it's ID
     *
     * @param id
     *          The ID of the snapshot
     * @return String
     * @throws RadosException
     */
    public String snapGetName(long id) throws RadosException {
        byte[] buf = new byte[512];
        int r = rados.rados_ioctx_snap_get_name(this.getPointer(), id, buf, buf.length);
        if (r < 0) {
            throw new RadosException("Failed to lookup the name of snapshot " + id, r);
        }
        return new String(buf).trim();
    }

    /**
     * Get the timestamp of a snapshot
     *
     * @param id
     *         The ID of the snapshot
     * @return long
     * @throws RadosException
     */
    public long snapGetStamp(long id) throws RadosException {
        LongByReference time = new LongByReference();
        int r = rados.rados_ioctx_snap_get_stamp(this.getPointer(), id, time);
        if (r < 0) {
            throw new RadosException("Failed to retrieve the timestamp of snapshot " + id, r);
        }
        return time.getValue();
    }

    /**
     * List all snapshots
     *
     * @return Long[]
     * @throws RadosException
     */
    public Long[] snapList() throws RadosException {
        byte[] buf = new byte[512];
        int r = rados.rados_ioctx_snap_list(this.getPointer(), buf, buf.length);
        if (r < 0) {
            throw new RadosException("Failed to list all snapshots", r);
        }

        Long[] snaps = new Long[r];
        for (int i = 0; i < r; i++) {
            snaps[i] = new Long(buf[i]);
        }
        return snaps;
    }
}
