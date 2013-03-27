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

import com.sun.jna.Structure;
import java.util.List;
import java.util.Arrays;

public class RadosClusterInfo extends Structure {
    public long kb;
    public long kb_used;
    public long kb_avail;
    public long num_objects;

    protected List getFieldOrder() {
        return Arrays.asList("kb", "kb_used", "kb_avail", "num_objects");
    }
}
