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

package com.ceph.rbd.jna;

import com.sun.jna.Structure;
import com.sun.jna.Pointer;
import java.util.List;
import java.util.Arrays;

public class RbdSnapInfo extends Structure {
    public long id;
    public long size;
    public String name;

    public RbdSnapInfo() {
        // Required for the toArray method
    }

    public RbdSnapInfo(Pointer p) {
        super(p);
    }

    protected List getFieldOrder() {
        return Arrays.asList("id", "size", "name");
    }
}
