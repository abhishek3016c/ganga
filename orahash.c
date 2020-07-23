/*
 * ocfshash.c
 *
 * Allows for creation and destruction of a hash table which one
 * can use to read, write and delete data.
 *
 * Copyright (C) 2002 Oracle Corporation.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA.
 *
 * Authors: Neeraj Goyal, Suchit Kaura, Kurt Hackel, Sunil Mushran,
 *          Manish Singh, Wim Coekaerts
 */

#include "orahash.h"
//#include "mylinuxtypes.h"
#include "sys/types.h"
/* Tracing */
#define OCFS_DEBUG_CONTEXT      OCFS_DEBUG_CONTEXT_HASH


/*
 * --------------------------------------------------------------------
 * hash() -- hash a variable-length key into a 32-bit value
 *   k       : the key (the unaligned variable-length array of bytes)
 *   len     : the length of the key, counting by bytes
 *   initval : can be any 4-byte value
 *
 * Returns a 32-bit value.  Every bit of the key affects every bit of
 * the return value.  Every 1-bit and 2-bit delta achieves avalanche.
 * About 6*len+35 instructions.
 *
 * The best hash table sizes are powers of 2.  There is no need to do
 * mod a prime (mod is sooo slow!).  If you need less than 32 bits,
 * use a bitmask.  For example, if you need only 10 bits, do
 * h = (h & hashmask(10));
 * In which case, the hash table should have hashsize(10) elements.
 *
 * If you are hashing n strings (__u8 **)k, do it like this:
 * for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);
 *
 * By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may use this
 * code any way you wish, private, educational, or commercial.  It's free.
 *
 * See http://burtleburtle.net/bob/hash/evahash.html
 * Use for hash table lookup, or anything where one collision in 2^^32 is
 * acceptable.  Do NOT use for cryptographic purposes.
 * --------------------------------------------------------------------
 */
__u32 hash (k, length, initval)
        register __u8 *k;		/* the key */
        register __u32 length;		/* the length of the key */
        register __u32 initval;		/* the previous hash, or an arbitrary value */
{
    register __u32 a, b, c, len;

    /* Set up the internal state */
    len = length;
    a = b = 0x9e3779b9;	/* the golden ratio; an arbitrary value */
    c = initval;		/* the previous hash value */

    /*---------------------------------------- handle most of the key */
    while (len >= 12) {
        a += (k[0] + ((__u32) k[1] << 8) + ((__u32) k[2] << 16) +
              ((__u32) k[3] << 24));
        b += (k[4] + ((__u32) k[5] << 8) + ((__u32) k[6] << 16) +
              ((__u32) k[7] << 24));
        c += (k[8] + ((__u32) k[9] << 8) + ((__u32) k[10] << 16) +
              ((__u32) k[11] << 24));
        mix (a, b, c);
        k += 12;
        len -= 12;
    }

    /*------------------------------------- handle the last 11 bytes */
    c += length;
    switch (len) {		/* all the case statements fall through */
        case 11:
            c += ((__u32) k[10] << 24);
        case 10:
            c += ((__u32) k[9] << 16);
        case 9:
            c += ((__u32) k[8] << 8);
            /* the first byte of c is reserved for the length */
        case 8:
            b += ((__u32) k[7] << 24);
        case 7:
            b += ((__u32) k[6] << 16);
        case 6:
            b += ((__u32) k[5] << 8);
        case 5:
            b += k[4];
        case 4:
            a += ((__u32) k[3] << 24);
        case 3:
            a += ((__u32) k[2] << 16);
        case 2:
            a += ((__u32) k[1] << 8);
        case 1:
            a += k[0];
            /* case 0: nothing left to add */
    }
    mix (a, b, c);
    /*-------------------------------------------- report the result */
    return c;
}				/* hash */
