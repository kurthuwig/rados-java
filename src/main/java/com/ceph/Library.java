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

import com.ceph.jna.Rados;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

final class Library {
    final static Rados rados;

    static {
        rados = Rados.INSTANCE;
    }

    private Library() {}

    /**
     * Free memory pointed to by ptr.
     */
    static void free(Pointer ptr) {
        Pointer.nativeValue(ptr, 0L);
    }

    /**
     * Convert the data pointed to by {@code ptr} to a String.
     */
    static String getString(Pointer ptr) {
        final long len = ptr.indexOf(0, (byte)0);
        assert (len != -1): "C-Strings must be \\0 terminated.";

        final byte[] data = ptr.getByteArray(0, (int)len);
        try {
            return new String(data, "utf-8");
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException("Rados problem: UTF-8 decoding error.", e);
        }
    }

    /**
     * Calls {@link #toStringArray(Pointer[], int)}.
     */
    static String[] toStringArray(Pointer[] ptrArr) {
        return toStringArray(ptrArr, ptrArr.length);
    }

    /**
     * Convert the given array of native pointers to "char" in
     * UTF-8 encoding to an array of Strings.
     *
     * note The memory used by the elements of the original array
     *       is freed and ptrArr is modified.
     */
    static String[] toStringArray(Pointer[] ptrArr, final int size) {
        try {
            final String[] result = new String[size];
            for (int i = 0; i < size; ++i) {
                result[i] = Library.getString(ptrArr[i]);
            }
            return result;
        } finally {
            for (int i = 0; i < size; ++i) {
                Library.free(ptrArr[i]);
                ptrArr[i] = null;
            }
        }
    }
}
