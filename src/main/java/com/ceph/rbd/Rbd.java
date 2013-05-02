/*
 * RADOS Java - Java bindings for librados and librbd
 *
 * Copyright (C) 2013 Wido den Hollander <wido@42on.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file LICENSE.
 *
 */

package com.ceph.rbd;

import com.ceph.rados.IoCTX;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.Pointer;
import com.sun.jna.Memory;
import com.sun.jna.Native;

import static com.ceph.rbd.Library.rbd;

public class Rbd {

    Pointer io;


    /**
     * Get the librbd version
     *
     * @return a int array with the minor, major and extra version
     */
    public static int[] getVersion() {
        IntByReference minor = new IntByReference();
        IntByReference major = new IntByReference();
        IntByReference extra = new IntByReference();
        rbd.rbd_version(minor, major, extra);
        int[] returnValue = {minor.getValue(), major.getValue(), extra.getValue()};
        return returnValue;
    }

    public Rbd(IoCTX io) {
        this.io = io.getPointer();
    }

    /**
     * Create a new RBD image
     *
     * @param name
     *         The name of the new image
     * @param size
     *         The size of the new image in bytes
     * @param order
     * @throws RbdException
     */
    public void create(String name, long size) throws RbdException {
        IntByReference order = new IntByReference();
        int r = rbd.rbd_create(this.io, name, size, order);
        if (r < 0) {
            throw new RbdException("Failed to create image " + name, r);
        }
    }

    /**
     * Remove a RBD image
     *
     * @param name
     *         The name of the image
     * @throws RbdException
     */
    public void remove(String name) throws RbdException {
        int r = rbd.rbd_remove(this.io, name);
        if (r < 0) {
            throw new RbdException("Failed to remove image " + name, r);
        }
    }

    /**
     * List all RBD images in this pool
     *
     * @return String[]
     * @throws RbdException
     */
    public String[] list() throws RbdException {
        IntByReference size = new IntByReference(1024);
        byte[] names;
        int r;

        while (true) {
            names = new byte[size.getValue()];
            r = rbd.rbd_list(this.io, names, size);
            if (r >= 0) {
                break;
            }

            /* -34 == ERANGE */
            if (r != -34) {
                throw new RbdException("Failed listing the RBD images", r);
            }
        }
        return Native.toString(names).split("\0");
    }

}