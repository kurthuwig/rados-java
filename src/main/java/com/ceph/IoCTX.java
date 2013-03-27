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

import com.sun.jna.Pointer;

import static com.ceph.Library.rados;

public class IoCTX {

    private Pointer ioCtxPtr;

    public IoCTX(Pointer p) {
        this.ioCtxPtr = p;
    }

    public Pointer getPointer() {
        return this.ioCtxPtr.getPointer(0);
    }

}
