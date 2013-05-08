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
import java.util.List;
import java.util.Arrays;

public class RbdImageInfo extends Structure implements Structure.ByReference {
    public long size;
    public long obj_size;
    public long num_objs;
    public int order;
    public byte[] block_name_prefix;
    public long parent_pool;
    public byte[] parent_name;

    public RbdImageInfo() {
        super();
        this.block_name_prefix = new byte[24];
        this.parent_name = new byte[96];
    }

    protected List getFieldOrder() {
        return Arrays.asList("size", "obj_size", "num_objs", "order",
                             "block_name_prefix", "parent_pool",
                             "parent_name");
    }
}
