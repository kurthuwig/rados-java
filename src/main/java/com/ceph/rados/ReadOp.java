/*
 * RADOS Java - Java bindings for librados
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * ***********
 * * history *
 * ***********
 * 2014-08-15 - initial implementation supporting ranged reads only
 */

package com.ceph.rados;

import static com.ceph.rados.Library.rados;

import java.nio.ByteBuffer;

import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.LongByReference;

public class ReadOp {

    public static class ReadResult {
        private final ByteBuffer buf;
        final LongByReference bytesread;
        final IntByReference rval;
        ReadResult(long buflen) throws RadosException {
            if ( buflen > Integer.MAX_VALUE ) {
                throw new RadosException("rados_read_op_read Java byte[] buffer cannot be longer than "+Integer.MAX_VALUE);
            }
            buf = ByteBuffer.allocateDirect((int) buflen);
            bytesread = new LongByReference();
            rval = new IntByReference();
        }
        public ByteBuffer getBuffer() { return buf; }
        public long getBytesRead() { return bytesread.getValue(); }
        public int  getRVal() { return rval.getValue(); }
    }

    private final Pointer ioctxPtr;
    private final Pointer readOpPtr;

    /**
     * Create a new read_op object.
     *
     * This constructor should never be called, ReadOp
     * objects are created by the IoCTX class and returned
     * when creating a ReadOp there.
     */
    ReadOp(Pointer ioctx_p, Pointer readop_p) {
        this.ioctxPtr = ioctx_p;
        this.readOpPtr = readop_p;
    }

    Pointer getPointer() {
        return readOpPtr;
    }
    
    /**
     * Add a read operation to the rados_read_op_t via rados_read_op_read.  Note returned
     * ReadResult is not populated until after the operate() call.
     * 
     * @param offset starting offset into the object
     * @param len length of the read
     * @return Java object which will hold results of the requested read after operate() is called
     * @throws RadosException
     */
    public ReadResult queueRead(long offset, long len) throws RadosException {
        ReadResult r = new ReadResult(len);
        rados.rados_read_op_read(readOpPtr, offset, len, r.getBuffer(), r.bytesread, r.rval);
        return r;
    }
    
    /**
     * Executes operations added to the rados_read_op_t.
     * 
     * @param oid
     * @param flags
     * @return rados_read_op_operate return value
     */
    public int operate(String oid, int flags) {
        return rados.rados_read_op_operate(readOpPtr, ioctxPtr, oid, flags);
    }
}