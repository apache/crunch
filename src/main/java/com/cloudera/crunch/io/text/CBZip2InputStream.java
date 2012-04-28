/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001-2003 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution, if
 *    any, must include the following acknowlegement:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowlegement may appear in the software itself,
 *    if and wherever such third-party acknowlegements normally appear.
 *
 * 4. The names "Ant" and "Apache Software
 *    Foundation" must not be used to endorse or promote products derived
 *    from this software without prior written permission. For written
 *    permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache"
 *    nor may "Apache" appear in their names without prior written
 *    permission of the Apache Group.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

/*
 * This package is based on the work done by Keiron Liddle, Aftex Software
 * <keiron@aftexsw.com> to whom the Ant project is very grateful for his
 * great code.
 */
package com.cloudera.crunch.io.text;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.bzip2.BZip2Constants;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * An input stream that decompresses from the BZip2 format (without the file
 * header chars) to be read as any other stream.
 *
 * @author <a href="mailto:keiron@aftexsw.com">Keiron Liddle</a>
 */
public class CBZip2InputStream extends InputStream implements BZip2Constants {
    private static void cadvise(String reason) throws IOException {
        throw new IOException(reason);
    }

    private static void compressedStreamEOF() throws IOException {
        cadvise("compressedStream EOF");
    }

    private void makeMaps() {
        int i;
        nInUse = 0;
        for (i = 0; i < 256; i++) {
            if (inUse[i]) {
                seqToUnseq[nInUse] = (char) i;
                unseqToSeq[i] = (char) nInUse;
                nInUse++;
            }
        }
    }

    /*
      index of the last char in the block, so
      the block size == last + 1.
    */
    private int  last;

    /*
      index in zptr[] of original string after sorting.
    */
    private int  origPtr;

    /*
      always: in the range 0 .. 9.
      The current block size is 100000 * this number.
    */
    private int blockSize100k;

    private boolean blockRandomised;

    // a buffer to keep the read byte
    private int bsBuff;
    
    // since bzip is bit-aligned at block boundaries there can be a case wherein
    // only few bits out of a read byte are consumed and the remaining bits
    // need to be consumed while processing the next block.
    // indicate how many bits in bsBuff have not been processed yet
    private int bsLive;
    private CRC mCrc = new CRC();

    private boolean[] inUse = new boolean[256];
    private int nInUse;

    private char[] seqToUnseq = new char[256];
    private char[] unseqToSeq = new char[256];

    private char[] selector = new char[MAX_SELECTORS];
    private char[] selectorMtf = new char[MAX_SELECTORS];

    private int[] tt;
    private char[] ll8;

    /*
      freq table collected to save a pass over the data
      during decompression.
    */
    private int[] unzftab = new int[256];

    private int[][] limit = new int[N_GROUPS][MAX_ALPHA_SIZE];
    private int[][] base = new int[N_GROUPS][MAX_ALPHA_SIZE];
    private int[][] perm = new int[N_GROUPS][MAX_ALPHA_SIZE];
    private int[] minLens = new int[N_GROUPS];

    private FSDataInputStream innerBsStream;
    long readLimit = Long.MAX_VALUE;
    public long getReadLimit() {
        return readLimit;
    }
    public void setReadLimit(long readLimit) {
        this.readLimit = readLimit;
    }
    long readCount;
    public long getReadCount() {
        return readCount;
    }

    private boolean streamEnd = false;

    private int currentChar = -1;

    private static final int START_BLOCK_STATE = 1;
    private static final int RAND_PART_A_STATE = 2;
    private static final int RAND_PART_B_STATE = 3;
    private static final int RAND_PART_C_STATE = 4;
    private static final int NO_RAND_PART_A_STATE = 5;
    private static final int NO_RAND_PART_B_STATE = 6;
    private static final int NO_RAND_PART_C_STATE = 7;

    private int currentState = START_BLOCK_STATE;

    private int storedBlockCRC, storedCombinedCRC;
    private int computedBlockCRC, computedCombinedCRC;
    private boolean checkComputedCombinedCRC = true;

    int i2, count, chPrev, ch2;
    int i, tPos;
    int rNToGo = 0;
    int rTPos  = 0;
    int j2;
    char z;
    
    // see comment in getPos()
    private long retPos = -1;
    // the position offset which corresponds to the end of the InputSplit that
    // will be processed by this instance 
    private long endOffsetOfSplit;

    private boolean signalToStopReading;

    public CBZip2InputStream(FSDataInputStream zStream, int blockSize, long end)
    throws IOException {
        endOffsetOfSplit = end;
        // initialize retPos to the beginning of the current InputSplit
        // see comments in getPos() to understand how this is used.
        retPos = zStream.getPos();
    	ll8 = null;
        tt = null;
        checkComputedCombinedCRC = blockSize == -1;
        bsSetStream(zStream);
        initialize(blockSize);
        initBlock(blockSize != -1);
        setupBlock();
    }

    @Override
    public int read() throws IOException {
        if (streamEnd) {
            return -1;
        } else {
            
            // if we just started reading a bzip block which starts at a position
            // >= end of current split, then we should set up retpos such that
            // after a record is read, future getPos() calls will get a value
            // > end of current split - this way we will read only one record out
            // of this bzip block - the rest of the records from this bzip block
            // should be read by the next map task while processing the next split
            if(signalToStopReading) {
                retPos = endOffsetOfSplit + 1;
            }
            
            int retChar = currentChar;
            switch(currentState) {
            case START_BLOCK_STATE:
                break;
            case RAND_PART_A_STATE:
                break;
            case RAND_PART_B_STATE:
                setupRandPartB();
                break;
            case RAND_PART_C_STATE:
                setupRandPartC();
                break;
            case NO_RAND_PART_A_STATE:
                break;
            case NO_RAND_PART_B_STATE:
                setupNoRandPartB();
                break;
            case NO_RAND_PART_C_STATE:
                setupNoRandPartC();
                break;
            default:
                break;
            }
            return retChar;
        }
    }

    /**
     * getPos is used by the caller to know when the processing of the current 
     * {@link InputSplit} is complete. In this method, as we read each bzip
     * block, we keep returning the beginning of the {@link InputSplit} as the
     * return value until we hit a block  which starts at a position >= end of
     * current split. At that point we should set up retpos such that after a 
     * record is read, future getPos() calls will get a value > end of current 
     * split - this way we will read only one record out of that bzip block - 
     * the rest of the records from that bzip block should be read by the next 
     * map task while processing the next split
     * @return
     * @throws IOException
     */
    public long getPos() throws IOException{
        return retPos;
    }
    
    private void initialize(int blockSize) throws IOException {
        if (blockSize == -1) {
            char magic1, magic2;
            char magic3, magic4;
            magic1 = bsGetUChar();
            magic2 = bsGetUChar();
            magic3 = bsGetUChar();
            magic4 = bsGetUChar();
            if (magic1 != 'B' || magic2 != 'Z' || 
                    magic3 != 'h' || magic4 < '1' || magic4 > '9') {
                bsFinishedWithStream();
                streamEnd = true;
                return;
            }
            blockSize = magic4 - '0';
        }

        setDecompressStructureSizes(blockSize);
        computedCombinedCRC = 0;
    }

    private final static long mask = 0xffffffffffffL;
    private final static long eob = 0x314159265359L & mask;
    private final static long eos = 0x177245385090L & mask;
    
    private void initBlock(boolean searchForMagic) throws IOException {
        if (readCount >= readLimit) {
            bsFinishedWithStream();
            streamEnd = true;
            return;
        }
        
        // position before beginning of bzip block header        
        long pos = innerBsStream.getPos();
        if (!searchForMagic) {
            char magic1, magic2, magic3, magic4;
            char magic5, magic6;
            magic1 = bsGetUChar();
            magic2 = bsGetUChar();
            magic3 = bsGetUChar();
            magic4 = bsGetUChar();
            magic5 = bsGetUChar();
            magic6 = bsGetUChar();
            if (magic1 == 0x17 && magic2 == 0x72 && magic3 == 0x45
                    && magic4 == 0x38 && magic5 == 0x50 && magic6 == 0x90) {
                complete();
                return;
            }

            if (magic1 != 0x31 || magic2 != 0x41 || magic3 != 0x59
                    || magic4 != 0x26 || magic5 != 0x53 || magic6 != 0x59) {
                badBlockHeader();
                streamEnd = true;
                return;
            }
        } else {
            long magic = 0;
            for(int i = 0; i < 6; i++) {
                magic <<= 8;
                magic |= bsGetUChar();
            }
            while(magic != eos && magic != eob) {
                magic <<= 1;
                magic &= mask;
                magic |= bsR(1);
                // if we just found the block header, the beginning of the bzip 
                // header would be 6 bytes before the current stream position
                // when we eventually break from this while(), if it is because
                // we found a block header then pos will have the correct start
                // of header position
                pos = innerBsStream.getPos() - 6;
            }
            if (magic == eos) {
                complete();
                return;
            }
            
        }
        // if the previous block finished a few bits into the previous byte,
        // then we will first be reading the remaining bits from the previous
        // byte - so logically pos needs to be one behind
        if(bsLive > 0)  {
            pos--;
        }
        
        if(pos >= endOffsetOfSplit) {
            // we have reached a block which begins exactly at the next InputSplit
            // or >1 byte into the next InputSplit - lets record this fact
            signalToStopReading = true;
        }
        storedBlockCRC = bsGetInt32();

        if (bsR(1) == 1) {
            blockRandomised = true;
        } else {
            blockRandomised = false;
        }

        //        currBlockNo++;
        getAndMoveToFrontDecode();

        mCrc.initialiseCRC();
        currentState = START_BLOCK_STATE;
    }

    private void endBlock() throws IOException {
        computedBlockCRC = mCrc.getFinalCRC();
        /* A bad CRC is considered a fatal error. */
        if (storedBlockCRC != computedBlockCRC) {
            crcError();
        }

        computedCombinedCRC = (computedCombinedCRC << 1)
            | (computedCombinedCRC >>> 31);
        computedCombinedCRC ^= computedBlockCRC;
    }

    private void complete() throws IOException {
        storedCombinedCRC = bsGetInt32();
        if (checkComputedCombinedCRC && 
                storedCombinedCRC != computedCombinedCRC) {
            crcError();
        }
        if (innerBsStream.getPos() < endOffsetOfSplit) {
        	throw new IOException("Encountered additional bytes in the filesplit past the crc block. "
        			+ "Loading of concatenated bz2 files is not supported");
        }
        bsFinishedWithStream();
        streamEnd = true;
    }

    private static void blockOverrun() throws IOException {
        cadvise("block overrun");
    }

    private static void badBlockHeader() throws IOException {
        cadvise("bad block header");
    }

    private static void crcError() throws IOException {
        cadvise("CRC error");
    }

    private void bsFinishedWithStream() {
        if (this.innerBsStream != null) {
            if (this.innerBsStream != System.in) {
                this.innerBsStream = null;
            }
        }
    }

    private void bsSetStream(FSDataInputStream f) {
        innerBsStream = f;
        bsLive = 0;
        bsBuff = 0;
    }

    final private int readBs() throws IOException {
        readCount++;
        return innerBsStream.read();
    }
    private int bsR(int n) throws IOException {
        int v;
        while (bsLive < n) {
            int zzi;
            zzi = readBs();
            if (zzi == -1) {
                compressedStreamEOF();
            }
            bsBuff = (bsBuff << 8) | (zzi & 0xff);
            bsLive += 8;
        }

        v = (bsBuff >> (bsLive - n)) & ((1 << n) - 1);
        bsLive -= n;
        return v;
    }
    

    private char bsGetUChar() throws IOException {
        return (char) bsR(8);
    }

    private int bsGetint() throws IOException {
        int u = 0;
        u = (u << 8) | bsR(8);
        u = (u << 8) | bsR(8);
        u = (u << 8) | bsR(8);
        u = (u << 8) | bsR(8);
        return u;
    }

    private int bsGetIntVS(int numBits) throws IOException {
        return bsR(numBits);
    }

    private int bsGetInt32() throws IOException {
        return bsGetint();
    }

    private void hbCreateDecodeTables(int[] limit, int[] base,
                                      int[] perm, char[] length,
                                      int minLen, int maxLen, int alphaSize) {
        int pp, i, j, vec;

        pp = 0;
        for (i = minLen; i <= maxLen; i++) {
            for (j = 0; j < alphaSize; j++) {
                if (length[j] == i) {
                    perm[pp] = j;
                    pp++;
                }
            }
        }

        for (i = 0; i < MAX_CODE_LEN; i++) {
            base[i] = 0;
        }
        for (i = 0; i < alphaSize; i++) {
            base[length[i] + 1]++;
        }

        for (i = 1; i < MAX_CODE_LEN; i++) {
            base[i] += base[i - 1];
        }

        for (i = 0; i < MAX_CODE_LEN; i++) {
            limit[i] = 0;
        }
        vec = 0;

        for (i = minLen; i <= maxLen; i++) {
            vec += (base[i + 1] - base[i]);
            limit[i] = vec - 1;
            vec <<= 1;
        }
        for (i = minLen + 1; i <= maxLen; i++) {
            base[i] = ((limit[i - 1] + 1) << 1) - base[i];
        }
    }

    private void recvDecodingTables() throws IOException {
        char len[][] = new char[N_GROUPS][MAX_ALPHA_SIZE];
        int i, j, t, nGroups, nSelectors, alphaSize;
        int minLen, maxLen;
        boolean[] inUse16 = new boolean[16];

        /* Receive the mapping table */
        for (i = 0; i < 16; i++) {
            if (bsR(1) == 1) {
                inUse16[i] = true;
            } else {
                inUse16[i] = false;
            }
        }

        for (i = 0; i < 256; i++) {
            inUse[i] = false;
        }

        for (i = 0; i < 16; i++) {
            if (inUse16[i]) {
                for (j = 0; j < 16; j++) {
                    if (bsR(1) == 1) {
                        inUse[i * 16 + j] = true;
                    }
                }
            }
        }

        makeMaps();
        alphaSize = nInUse + 2;

        /* Now the selectors */
        nGroups = bsR(3);
        nSelectors = bsR(15);
        for (i = 0; i < nSelectors; i++) {
            j = 0;
            while (bsR(1) == 1) {
                j++;
            }
            selectorMtf[i] = (char) j;
        }

        /* Undo the MTF values for the selectors. */
        {
            char[] pos = new char[N_GROUPS];
            char tmp, v;
            for (v = 0; v < nGroups; v++) {
                pos[v] = v;
            }

            for (i = 0; i < nSelectors; i++) {
                v = selectorMtf[i];
                tmp = pos[v];
                while (v > 0) {
                    pos[v] = pos[v - 1];
                    v--;
                }
                pos[0] = tmp;
                selector[i] = tmp;
            }
        }

        /* Now the coding tables */
        for (t = 0; t < nGroups; t++) {
            int curr = bsR(5);
            for (i = 0; i < alphaSize; i++) {
                while (bsR(1) == 1) {
                    if (bsR(1) == 0) {
                        curr++;
                    } else {
                        curr--;
                    }
                }
                len[t][i] = (char) curr;
            }
        }

        /* Create the Huffman decoding tables */
        for (t = 0; t < nGroups; t++) {
            minLen = 32;
            maxLen = 0;
            for (i = 0; i < alphaSize; i++) {
                if (len[t][i] > maxLen) {
                    maxLen = len[t][i];
                }
                if (len[t][i] < minLen) {
                    minLen = len[t][i];
                }
            }
            hbCreateDecodeTables(limit[t], base[t], perm[t], len[t], minLen,
                                 maxLen, alphaSize);
            minLens[t] = minLen;
        }
    }

    private void getAndMoveToFrontDecode() throws IOException {
        char[] yy = new char[256];
        int i, j, nextSym, limitLast;
        int EOB, groupNo, groupPos;

        limitLast = baseBlockSize * blockSize100k;
        origPtr = bsGetIntVS(24);

        recvDecodingTables();
        EOB = nInUse + 1;
        groupNo = -1;
        groupPos = 0;

        /*
          Setting up the unzftab entries here is not strictly
          necessary, but it does save having to do it later
          in a separate pass, and so saves a block's worth of
          cache misses.
        */
        for (i = 0; i <= 255; i++) {
            unzftab[i] = 0;
        }

        for (i = 0; i <= 255; i++) {
            yy[i] = (char) i;
        }

        last = -1;

        {
            int zt, zn, zvec, zj;
            if (groupPos == 0) {
                groupNo++;
                groupPos = G_SIZE;
            }
            groupPos--;
            zt = selector[groupNo];
            zn = minLens[zt];
            zvec = bsR(zn);
            while (zvec > limit[zt][zn]) {
                zn++;
                {
                    {
                        while (bsLive < 1) {
                            int zzi = 0;
                            try {
                                zzi = readBs();
                            } catch (IOException e) {
                                compressedStreamEOF();
                            }
                            if (zzi == -1) {
                                compressedStreamEOF();
                            }
                            bsBuff = (bsBuff << 8) | (zzi & 0xff);
                            bsLive += 8;
                        }
                    }
                    zj = (bsBuff >> (bsLive - 1)) & 1;
                    bsLive--;
                }
                zvec = (zvec << 1) | zj;
            }
            nextSym = perm[zt][zvec - base[zt][zn]];
        }

        while (true) {

            if (nextSym == EOB) {
                break;
            }

            if (nextSym == RUNA || nextSym == RUNB) {
                char ch;
                int s = -1;
                int N = 1;
                do {
                    if (nextSym == RUNA) {
                        s = s + (0 + 1) * N;
                    } else if (nextSym == RUNB) {
                        s = s + (1 + 1) * N;
                           }
                    N = N * 2;
                    {
                        int zt, zn, zvec, zj;
                        if (groupPos == 0) {
                            groupNo++;
                            groupPos = G_SIZE;
                        }
                        groupPos--;
                        zt = selector[groupNo];
                        zn = minLens[zt];
                        zvec = bsR(zn);
                        while (zvec > limit[zt][zn]) {
                            zn++;
                            {
                                {
                                    while (bsLive < 1) {
                                        int zzi = 0;
                                        try {
                                            zzi = readBs();
                                        } catch (IOException e) {
                                            compressedStreamEOF();
                                        }
                                        if (zzi == -1) {
                                            compressedStreamEOF();
                                        }
                                        bsBuff = (bsBuff << 8) | (zzi & 0xff);
                                        bsLive += 8;
                                    }
                                }
                                zj = (bsBuff >> (bsLive - 1)) & 1;
                                bsLive--;
                            }
                            zvec = (zvec << 1) | zj;
                        }
                        nextSym = perm[zt][zvec - base[zt][zn]];
                    }
                } while (nextSym == RUNA || nextSym == RUNB);

                s++;
                ch = seqToUnseq[yy[0]];
                unzftab[ch] += s;

                while (s > 0) {
                    last++;
                    ll8[last] = ch;
                    s--;
                }

                if (last >= limitLast) {
                    blockOverrun();
                }
                continue;
            } else {
                char tmp;
                last++;
                if (last >= limitLast) {
                    blockOverrun();
                }

                tmp = yy[nextSym - 1];
                unzftab[seqToUnseq[tmp]]++;
                ll8[last] = seqToUnseq[tmp];

                /*
                  This loop is hammered during decompression,
                  hence the unrolling.

                  for (j = nextSym-1; j > 0; j--) yy[j] = yy[j-1];
                */

                j = nextSym - 1;
                for (; j > 3; j -= 4) {
                    yy[j]     = yy[j - 1];
                    yy[j - 1] = yy[j - 2];
                    yy[j - 2] = yy[j - 3];
                    yy[j - 3] = yy[j - 4];
                }
                for (; j > 0; j--) {
                    yy[j] = yy[j - 1];
                }

                yy[0] = tmp;
                {
                    int zt, zn, zvec, zj;
                    if (groupPos == 0) {
                        groupNo++;
                        groupPos = G_SIZE;
                    }
                    groupPos--;
                    zt = selector[groupNo];
                    zn = minLens[zt];
                    zvec = bsR(zn);
                    while (zvec > limit[zt][zn]) {
                        zn++;
                        {
                            {
                                while (bsLive < 1) {
                                    int zzi;
                                    char thech = 0;
                                    try {
                                        thech = (char) readBs();
                                    } catch (IOException e) {
                                        compressedStreamEOF();
                                    }
                                    zzi = thech;
                                    bsBuff = (bsBuff << 8) | (zzi & 0xff);
                                    bsLive += 8;
                                }
                            }
                            zj = (bsBuff >> (bsLive - 1)) & 1;
                            bsLive--;
                        }
                        zvec = (zvec << 1) | zj;
                    }
                    nextSym = perm[zt][zvec - base[zt][zn]];
                }
                continue;
            }
        }
    }

    private void setupBlock() throws IOException {
        int[] cftab = new int[257];
        char ch;

        cftab[0] = 0;
        for (i = 1; i <= 256; i++) {
            cftab[i] = unzftab[i - 1];
        }
        for (i = 1; i <= 256; i++) {
            cftab[i] += cftab[i - 1];
        }

        for (i = 0; i <= last; i++) {
            ch = ll8[i];
            tt[cftab[ch]] = i;
            cftab[ch]++;
        }
        cftab = null;

        tPos = tt[origPtr];

        count = 0;
        i2 = 0;
        ch2 = 256;   /* not a char and not EOF */

        if (blockRandomised) {
            rNToGo = 0;
            rTPos = 0;
            setupRandPartA();
        } else {
            setupNoRandPartA();
        }
    }

    private void setupRandPartA() throws IOException {
        if (i2 <= last) {
            chPrev = ch2;
            ch2 = ll8[tPos];
            tPos = tt[tPos];
            if (rNToGo == 0) {
                rNToGo = rNums[rTPos];
                rTPos++;
                if (rTPos == 512) {
                    rTPos = 0;
                }
            }
            rNToGo--;
            ch2 ^= ((rNToGo == 1) ? 1 : 0);
            i2++;

            currentChar = ch2;
            currentState = RAND_PART_B_STATE;
            mCrc.updateCRC(ch2);
        } else {
            endBlock();
            initBlock(false);
            setupBlock();
        }
    }

    private void setupNoRandPartA() throws IOException {
        if (i2 <= last) {
            chPrev = ch2;
            ch2 = ll8[tPos];
            tPos = tt[tPos];
            i2++;

            currentChar = ch2;
            currentState = NO_RAND_PART_B_STATE;
            mCrc.updateCRC(ch2);
        } else {
            endBlock();
            initBlock(false);
            setupBlock();
        }
    }

    private void setupRandPartB() throws IOException {
        if (ch2 != chPrev) {
            currentState = RAND_PART_A_STATE;
            count = 1;
            setupRandPartA();
        } else {
            count++;
            if (count >= 4) {
                z = ll8[tPos];
                tPos = tt[tPos];
                if (rNToGo == 0) {
                    rNToGo = rNums[rTPos];
                    rTPos++;
                    if (rTPos == 512) {
                        rTPos = 0;
                    }
                }
                rNToGo--;
                z ^= ((rNToGo == 1) ? 1 : 0);
                j2 = 0;
                currentState = RAND_PART_C_STATE;
                setupRandPartC();
            } else {
                currentState = RAND_PART_A_STATE;
                setupRandPartA();
            }
        }
    }

    private void setupRandPartC() throws IOException {
        if (j2 < (int) z) {
            currentChar = ch2;
            mCrc.updateCRC(ch2);
            j2++;
        } else {
            currentState = RAND_PART_A_STATE;
            i2++;
            count = 0;
            setupRandPartA();
        }
    }

    private void setupNoRandPartB() throws IOException {
        if (ch2 != chPrev) {
            currentState = NO_RAND_PART_A_STATE;
            count = 1;
            setupNoRandPartA();
        } else {
            count++;
            if (count >= 4) {
                z = ll8[tPos];
                tPos = tt[tPos];
                currentState = NO_RAND_PART_C_STATE;
                j2 = 0;
                setupNoRandPartC();
            } else {
                currentState = NO_RAND_PART_A_STATE;
                setupNoRandPartA();
            }
        }
    }

    private void setupNoRandPartC() throws IOException {
        if (j2 < (int) z) {
            currentChar = ch2;
            mCrc.updateCRC(ch2);
            j2++;
        } else {
            currentState = NO_RAND_PART_A_STATE;
            i2++;
            count = 0;
            setupNoRandPartA();
        }
    }

    private void setDecompressStructureSizes(int newSize100k) {
        if (!(0 <= newSize100k && newSize100k <= 9 && 0 <= blockSize100k
               && blockSize100k <= 9)) {
            // throw new IOException("Invalid block size");
        }

        blockSize100k = newSize100k;

        if (newSize100k == 0) {
            return;
        }

        int n = baseBlockSize * newSize100k;
        ll8 = new char[n];
        tt = new int[n];
    }

    private static class CRC {
      public static int crc32Table[] = {
        0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9,
        0x130476dc, 0x17c56b6b, 0x1a864db2, 0x1e475005,
        0x2608edb8, 0x22c9f00f, 0x2f8ad6d6, 0x2b4bcb61,
        0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd,
        0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9,
        0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75,
        0x6a1936c8, 0x6ed82b7f, 0x639b0da6, 0x675a1011,
        0x791d4014, 0x7ddc5da3, 0x709f7b7a, 0x745e66cd,
        0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039,
        0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5,
        0xbe2b5b58, 0xbaea46ef, 0xb7a96036, 0xb3687d81,
        0xad2f2d84, 0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d,
        0xd4326d90, 0xd0f37027, 0xddb056fe, 0xd9714b49,
        0xc7361b4c, 0xc3f706fb, 0xceb42022, 0xca753d95,
        0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1,
        0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d,
        0x34867077, 0x30476dc0, 0x3d044b19, 0x39c556ae,
        0x278206ab, 0x23431b1c, 0x2e003dc5, 0x2ac12072,
        0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16,
        0x018aeb13, 0x054bf6a4, 0x0808d07d, 0x0cc9cdca,
        0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde,
        0x6b93dddb, 0x6f52c06c, 0x6211e6b5, 0x66d0fb02,
        0x5e9f46bf, 0x5a5e5b08, 0x571d7dd1, 0x53dc6066,
        0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
        0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e,
        0xbfa1b04b, 0xbb60adfc, 0xb6238b25, 0xb2e29692,
        0x8aad2b2f, 0x8e6c3698, 0x832f1041, 0x87ee0df6,
        0x99a95df3, 0x9d684044, 0x902b669d, 0x94ea7b2a,
        0xe0b41de7, 0xe4750050, 0xe9362689, 0xedf73b3e,
        0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
        0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686,
        0xd5b88683, 0xd1799b34, 0xdc3abded, 0xd8fba05a,
        0x690ce0ee, 0x6dcdfd59, 0x608edb80, 0x644fc637,
        0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb,
        0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f,
        0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53,
        0x251d3b9e, 0x21dc2629, 0x2c9f00f0, 0x285e1d47,
        0x36194d42, 0x32d850f5, 0x3f9b762c, 0x3b5a6b9b,
        0x0315d626, 0x07d4cb91, 0x0a97ed48, 0x0e56f0ff,
        0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623,
        0xf12f560e, 0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7,
        0xe22b20d2, 0xe6ea3d65, 0xeba91bbc, 0xef68060b,
        0xd727bbb6, 0xd3e6a601, 0xdea580d8, 0xda649d6f,
        0xc423cd6a, 0xc0e2d0dd, 0xcda1f604, 0xc960ebb3,
        0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7,
        0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b,
        0x9b3660c6, 0x9ff77d71, 0x92b45ba8, 0x9675461f,
        0x8832161a, 0x8cf30bad, 0x81b02d74, 0x857130c3,
        0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640,
        0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c,
        0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8,
        0x68860bfd, 0x6c47164a, 0x61043093, 0x65c52d24,
        0x119b4be9, 0x155a565e, 0x18197087, 0x1cd86d30,
        0x029f3d35, 0x065e2082, 0x0b1d065b, 0x0fdc1bec,
        0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088,
        0x2497d08d, 0x2056cd3a, 0x2d15ebe3, 0x29d4f654,
        0xc5a92679, 0xc1683bce, 0xcc2b1d17, 0xc8ea00a0,
        0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb, 0xdbee767c,
        0xe3a1cbc1, 0xe760d676, 0xea23f0af, 0xeee2ed18,
        0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
        0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0,
        0x9abc8bd5, 0x9e7d9662, 0x933eb0bb, 0x97ffad0c,
        0xafb010b1, 0xab710d06, 0xa6322bdf, 0xa2f33668,
        0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4
    };

    public CRC() {
        initialiseCRC();
    }

    void initialiseCRC() {
        globalCrc = 0xffffffff;
    }

    int getFinalCRC() {
        return ~globalCrc;
    }

    int getGlobalCRC() {
        return globalCrc;
    }

    void setGlobalCRC(int newCrc) {
        globalCrc = newCrc;
    }

    void updateCRC(int inCh) {
        int temp = (globalCrc >> 24) ^ inCh;
        if (temp < 0) {
            temp = 256 + temp;
        }
        globalCrc = (globalCrc << 8) ^ CRC.crc32Table[temp];
    }

    int globalCrc;
    }
}