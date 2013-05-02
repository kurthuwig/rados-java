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

public class RbdException extends Exception {

    protected int returnValue;

    /**
     * Throw a a RbdException
     *
     * @param message
     *         The error message
     */
    public RbdException(String message) {
        super(message);
    }

    /**
     * Throw a a RbdException
     *
     * @param message
     *         The error message
     * @param returnValue
     *         The return value of the rados_ call
     */
    public RbdException(String message, int returnValue) {
        super(message);
        this.returnValue = returnValue;
    }

    /**
     * Get the return value passed on to the constructor
     *
     * @return int
     */
    public int getReturnValue() {
        return this.returnValue;
    }

}
