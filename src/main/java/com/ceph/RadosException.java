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

public class RadosException extends Exception {

    protected int returnValue;

    public RadosException(String message) {
        super(message);
    }

    public RadosException(String message, int returnValue) {
        super(message);
        this.returnValue = returnValue;
    }

    public int getReturnValue() {
        return this.returnValue;
    }

}
