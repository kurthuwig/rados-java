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

import com.sun.jna.Structure;
import java.util.List;
import java.util.Arrays;

public class RadosPoolInfo extends Structure {
    public long num_bytes;
    public long num_kb;
    public long num_objects;
    public long num_object_clones;
    public long num_object_copies;
    public long num_objects_missing_on_primary;
    public long num_objects_unfound;
    public long num_objects_degraded;
    public long num_rd;
    public long num_rd_kb;
    public long num_wr;
    public long num_wr_kb;

    protected List getFieldOrder() {
        return Arrays.asList("num_bytes", "num_kb", "num_objects", "num_object_clones",
                             "num_object_copies", "num_objects_missing_on_primary",
                             "num_objects_unfound", "num_objects_degraded",
                             "num_rd", "num_rd_kb", "num_wr", "num_wr_kb");
    }
}