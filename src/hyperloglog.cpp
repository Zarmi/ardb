/* hyperloglog.c - Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "db.hpp"
#include "ardb_server.hpp"
#include "util/sds.h"

#include <stdint.h>
#include <math.h>

/* The Redis HyperLogLog implementation is based on the following ideas:
 *
 * * The use of a 64 bit hash function as proposed in [1], in order to don't
 *   limited to cardinalities up to 10^9, at the cost of just 1 additional
 *   bit per register.
 * * The use of 16384 6-bit registers for a great level of accuracy, using
 *   a total of 12k per key.
 * * The use of the Redis string data type. No new type is introduced.
 * * No attempt is made to compress the data structure as in [1]. Also the
 *   algorithm used is the original HyperLogLog Algorithm as in [2], with
 *   the only difference that a 64 bit hash function is used, so no correction
 *   is performed for values near 2^32 as in [1].
 *
 * [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
 *     Engineering of a State of The Art Cardinality Estimation Algorithm.
 *
 * [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 * Redis uses two representations:
 *
 * 1) A "dense" representation where every entry is represented by
 *    a 6-bit integer.
 * 2) A "sparse" representation using run length compression suitable
 *    for representing HyperLogLogs with many registers set to 0 in
 *    a memory efficient way.
 *
 *
 * HLL header
 * ===
 *
 * Both the dense and sparse representation have a 16 byte header as follows:
 *
 * +------+---+-----+----------+
 * | HYLL | E | N/U | Cardin.  |
 * +------+---+-----+----------+
 *
 * The first 4 bytes are a magic string set to the bytes "HYLL".
 * "E" is one byte encoding, currently set to HLL_DENSE or
 * HLL_SPARSE. N/U are three not used bytes.
 *
 * The "Cardin." field is a 64 bit integer stored in little endian format
 * with the latest cardinality computed that can be reused if the data
 * structure was not modified since the last computation (this is useful
 * because there are high probabilities that HLLADD operations don't
 * modify the actual data structure and hence the approximated cardinality).
 *
 * When the most significant bit in the most significant byte of the cached
 * cardinality is set, it means that the data structure was modified and
 * we can't reuse the cached value that must be recomputed.
 *
 * Dense representation
 * ===
 *
 * The dense representation used by Redis is the following:
 *
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * The 6 bits counters are encoded one after the other starting from the
 * LSB to the MSB, and using the next bytes as needed.
 *
 * Sparse representation
 * ===
 *
 * The sparse representation encodes registers using a run length
 * encoding composed of three opcodes, two using one byte, and one using
 * of two bytes. The opcodes are called ZERO, XZERO and VAL.
 *
 * ZERO opcode is represented as 00xxxxxx. The 6-bit integer represented
 * by the six bits 'xxxxxx', plus 1, means that there are N registers set
 * to 0. This opcode can represent from 1 to 64 contiguous registers set
 * to the value of 0.
 *
 * XZERO opcode is represented by two bytes 01xxxxxx yyyyyyyy. The 14-bit
 * integer represented by the bits 'xxxxxx' as most significant bits and
 * 'yyyyyyyy' as least significant bits, plus 1, means that there are N
 * registers set to 0. This opcode can represent from 0 to 16384 contiguous
 * registers set to the value of 0.
 *
 * VAL opcode is represented as 1vvvvvxx. It contains a 5-bit integer
 * representing the value of a register, and a 2-bit integer representing
 * the number of contiguous registers set to that value 'vvvvv'.
 * To obtain the value and run length, the integers vvvvv and xx must be
 * incremented by one. This opcode can represent values from 1 to 32,
 * repeated from 1 to 4 times.
 *
 * The sparse representation can't represent registers with a value greater
 * than 32, however it is very unlikely that we find such a register in an
 * HLL with a cardinality where the sparse representation is still more
 * memory efficient than the dense representation. When this happens the
 * HLL is converted to the dense representation.
 *
 * The sparse representation is purely positional. For example a sparse
 * representation of an empty HLL is just: XZERO:16384.
 *
 * An HLL having only 3 non-zero registers at position 1000, 1020, 1021
 * respectively set to 2, 3, 3, is represented by the following three
 * opcodes:
 *
 * XZERO:1000 (Registers 0-999 are set to 0)
 * VAL:2,1    (1 register set to value 2, that is register 1000)
 * ZERO:19    (Registers 1001-1019 set to 0)
 * VAL:3,2    (2 registers set to value 3, that is registers 1020,1021)
 * XZERO:15362 (Registers 1022-16383 set to 0)
 *
 * In the example the sparse representation used just 7 bytes instead
 * of 12k in order to represent the HLL registers. In general for low
 * cardinality there is a big win in terms of space efficiency, traded
 * with CPU time since the sparse representation is slower to access:
 *
 * The following table shows average cardinality vs bytes used, 100
 * samples per cardinality (when the set was not representable because
 * of registers with too big value, the dense representation size was used
 * as a sample).
 *
 * 100 267
 * 200 485
 * 300 678
 * 400 859
 * 500 1033
 * 600 1205
 * 700 1375
 * 800 1544
 * 900 1713
 * 1000 1882
 * 2000 3480
 * 3000 4879
 * 4000 6089
 * 5000 7138
 * 6000 8042
 * 7000 8823
 * 8000 9500
 * 9000 10088
 * 10000 10591
 *
 * The dense representation uses 12288 bytes, so there is a big win up to
 * a cardinality of ~2000-3000. For bigger cardinalities the constant times
 * involved in updating the sparse representation is not justified by the
 * memory savings. The exact maximum length of the sparse representation
 * when this implementation switches to the dense representation is
 * configured via the define server.hll_sparse_max_bytes.
 */

struct hllhdr
{
        char magic[4]; /* "HYLL" */
        uint8_t encoding; /* HLL_DENSE or HLL_SPARSE. */
        uint8_t notused[3]; /* Reserved for future use, must be zero. */
        uint8_t card[8]; /* Cached cardinality, little endian. */
        uint8_t registers[]; /* Data bytes. */
};

/* The cached cardinality MSB is used to signal validity of the cached value. */
#define HLL_INVALIDATE_CACHE(hdr) (hdr)->card[0] |= (1<<7)
#define HLL_VALID_CACHE(hdr) (((hdr)->card[0] & (1<<7)) == 0)

#define HLL_P 14 /* The greater is P, the smaller the error. */
#define HLL_REGISTERS (1<<HLL_P) /* With P=14, 16384 registers. */
#define HLL_P_MASK (HLL_REGISTERS-1) /* Mask to index register. */
#define HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX ((1<<HLL_BITS)-1)
#define HLL_HDR_SIZE sizeof(struct hllhdr)
#define HLL_DENSE_SIZE (HLL_HDR_SIZE+((HLL_REGISTERS*HLL_BITS+7)/8))
#define HLL_DENSE 0 /* Dense encoding. */
#define HLL_SPARSE 1 /* Sparse encoding. */
#define HLL_RAW 255 /* Only used internally, never exposed. */
#define HLL_MAX_ENCODING 1

//static char *invalid_hll_err = "-INVALIDOBJ Corrupted HLL object detected\r\n";

/* =========================== Low level bit macros ========================= */

/* Macros to access the dense representation.
 *
 * We need to get and set 6 bit counters in an array of 8 bit bytes.
 * We use macros to make sure the code is inlined since speed is critical
 * especially in order to compute the approximated cardinality in
 * HLLCOUNT where we need to access all the registers at once.
 * For the same reason we also want to avoid conditionals in this code path.
 *
 * +--------+--------+--------+------//
 * |11000000|22221111|33333322|55444444
 * +--------+--------+--------+------//
 *
 * Note: in the above representation the most significant bit (MSB)
 * of every byte is on the left. We start using bits from the LSB to MSB,
 * and so forth passing to the next byte.
 *
 * Example, we want to access to counter at pos = 1 ("111111" in the
 * illustration above).
 *
 * The index of the first byte b0 containing our data is:
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * The position of the first bit (counting from the LSB = 0) in the byte
 * is given by:
 *
 *  fb = 6 * pos % 8 -> 6
 *
 * Right shift b0 of 'fb' bits.
 *
 *   +--------+
 *   |11000000|  <- Initial value of b0
 *   |00000011|  <- After right shift of 6 pos.
 *   +--------+
 *
 * Left shift b1 of bits 8-fb bits (2 bits)
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   |22111100|  <- After left shift of 2 bits.
 *   +--------+
 *
 * OR the two bits, and finally AND with 111111 (63 in decimal) to
 * clean the higher order bits we are not interested in:
 *
 *   +--------+
 *   |00000011|  <- b0 right shifted
 *   |22111100|  <- b1 left shifted
 *   |22111111|  <- b0 OR b1
 *   |  111111|  <- (b0 OR b1) AND 63, our value.
 *   +--------+
 *
 * We can try with a different example, like pos = 0. In this case
 * the 6-bit counter is actually contained in a single byte.
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 *  fb = 6 * pos % 8 = 0
 *
 *  So we right shift of 0 bits (no shift in practice) and
 *  left shift the next byte of 8 bits, even if we don't use it,
 *  but this has the effect of clearing the bits so the result
 *  will not be affacted after the OR.
 *
 * -------------------------------------------------------------------------
 *
 * Setting the register is a bit more complex, let's assume that 'val'
 * is the value we want to set, already in the right range.
 *
 * We need two steps, in one we need to clear the bits, and in the other
 * we need to bitwise-OR the new bits.
 *
 * Let's try with 'pos' = 1, so our first byte at 'b' is 0,
 *
 * "fb" is 6 in this case.
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * To create a AND-mask to clear the bits about this position, we just
 * initialize the mask with the value 63, left shift it of "fs" bits,
 * and finally invert the result.
 *
 *   +--------+
 *   |00111111|  <- "mask" starts at 63
 *   |11000000|  <- "mask" after left shift of "ls" bits.
 *   |00111111|  <- "mask" after invert.
 *   +--------+
 *
 * Now we can bitwise-AND the byte at "b" with the mask, and bitwise-OR
 * it with "val" left-shifted of "ls" bits to set the new bits.
 *
 * Now let's focus on the next byte b1:
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   +--------+
 *
 * To build the AND mask we start again with the 63 value, right shift
 * it by 8-fb bits, and invert it.
 *
 *   +--------+
 *   |00111111|  <- "mask" set at 2&6-1
 *   |00001111|  <- "mask" after the right shift by 8-fb = 2 bits
 *   |11110000|  <- "mask" after bitwise not.
 *   +--------+
 *
 * Now we can mask it with b+1 to clear the old bits, and bitwise-OR
 * with "val" left-shifted by "rs" bits to set the new value.
 */

/* Note: if we access the last counter, we will also access the b+1 byte
 * that is out of the array, but sds strings always have an implicit null
 * term, so the byte exists, and we can skip the conditional (or the need
 * to allocate 1 byte more explicitly). */

/* Store the value of the register at position 'regnum' into variable 'target'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_GET_REGISTER(target,p,regnum) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long b0 = _p[_byte]; \
    unsigned long b1 = _p[_byte+1]; \
    target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
} while(0)

/* Set the value of the register at position 'regnum' to 'val'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_SET_REGISTER(p,regnum,val) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long _v = val; \
    _p[_byte] &= ~(HLL_REGISTER_MAX << _fb); \
    _p[_byte] |= _v << _fb; \
    _p[_byte+1] &= ~(HLL_REGISTER_MAX >> _fb8); \
    _p[_byte+1] |= _v >> _fb8; \
} while(0)

/* Macros to access the sparse representation.
 * The macros parameter is expected to be an uint8_t pointer. */
#define HLL_SPARSE_XZERO_BIT 0x40 /* 01xxxxxx */
#define HLL_SPARSE_VAL_BIT 0x80 /* 1vvvvvxx */
#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f)+1)
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p)+1)))+1)
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f)+1)
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3)+1)
#define HLL_SPARSE_VAL_MAX_VALUE 32
#define HLL_SPARSE_VAL_MAX_LEN 4
#define HLL_SPARSE_ZERO_MAX_LEN 64
#define HLL_SPARSE_XZERO_MAX_LEN 16384
#define HLL_SPARSE_VAL_SET(p,val,len) do { \
    *(p) = (((val)-1)<<2|((len)-1))|HLL_SPARSE_VAL_BIT; \
} while(0)
#define HLL_SPARSE_ZERO_SET(p,len) do { \
    *(p) = (len)-1; \
} while(0)
#define HLL_SPARSE_XZERO_SET(p,len) do { \
    int _l = (len)-1; \
    *(p) = (_l>>8) | HLL_SPARSE_XZERO_BIT; \
    *((p)+1) = (_l&0xff); \
} while(0)

/* ========================= HyperLogLog algorithm  ========================= */

/* Our hash function is MurmurHash2, 64 bit version.
 * It was modified for Redis in order to provide the same result in
 * big and little endian archs (endian neutral). */
uint64_t MurmurHash64A(const void * key, int len, unsigned int seed)
{
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t *data = (const uint8_t *) key;
    const uint8_t *end = data + (len - (len & 7));

    while (data != end)
    {
        uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
        k = *((uint64_t*) data);
#else
        k = (uint64_t) data[0];
        k |= (uint64_t) data[1] << 8;
        k |= (uint64_t) data[2] << 16;
        k |= (uint64_t) data[3] << 24;
        k |= (uint64_t) data[4] << 32;
        k |= (uint64_t) data[5] << 40;
        k |= (uint64_t) data[6] << 48;
        k |= (uint64_t) data[7] << 56;
#endif

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch (len & 7)
    {
        case 7:
            h ^= (uint64_t) data[6] << 48;
        case 6:
            h ^= (uint64_t) data[5] << 40;
        case 5:
            h ^= (uint64_t) data[4] << 32;
        case 4:
            h ^= (uint64_t) data[3] << 24;
        case 3:
            h ^= (uint64_t) data[2] << 16;
        case 2:
            h ^= (uint64_t) data[1] << 8;
        case 1:
            h ^= (uint64_t) data[0];
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

/* Given a string element to add to the HyperLogLog, returns the length
 * of the pattern 000..1 of the element hash. As a side effect 'regp' is
 * set to the register index this element hashes to. */
int hllPatLen(unsigned char *ele, size_t elesize, long *regp)
{
    uint64_t hash, bit, index;
    int count;

    /* Count the number of zeroes starting from bit HLL_REGISTERS
     * (that is a power of two corresponding to the first bit we don't use
     * as index). The max run can be 64-P+1 bits.
     *
     * Note that the final "1" ending the sequence of zeroes must be
     * included in the count, so if we find "001" the count is 3, and
     * the smallest count possible is no zeroes at all, just a 1 bit
     * at the first position, that is a count of 1.
     *
     * This may sound like inefficient, but actually in the average case
     * there are high probabilities to find a 1 after a few iterations. */
    hash = MurmurHash64A(ele, elesize, 0xadc83b19ULL);
    index = hash & HLL_P_MASK; /* Register index. */
    hash |= ((uint64_t) 1 << 63); /* Make sure the loop terminates. */
    bit = HLL_REGISTERS; /* First bit not used to address the register. */
    count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
    while ((hash & bit) == 0)
    {
        count++;
        bit <<= 1;
    }
    *regp = (int) index;
    return count;
}

/* ================== Dense representation implementation  ================== */

/* "Add" the element in the dense hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * 'registers' is expected to have room for HLL_REGISTERS plus an
 * additional byte on the right. This requirement is met by sds strings
 * automatically since they are implicitly null terminated.
 *
 * The function always succeed, however if as a result of the operation
 * the approximated cardinality changed, 1 is returned. Otherwise 0
 * is returned. */
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize)
{
    uint8_t oldcount, count;
    long index;

    /* Update the register if this element produced a longer run of zeroes. */
    count = hllPatLen(ele, elesize, &index);
    HLL_DENSE_GET_REGISTER(oldcount, registers, index);
    if (count > oldcount)
    {
        HLL_DENSE_SET_REGISTER(registers, index, count);
        return 1;
    }
    else
    {
        return 0;
    }
}

/* Compute SUM(2^-reg) in the dense representation.
 * PE is an array with a pre-computer table of values 2^-reg indexed by reg.
 * As a side effect the integer pointed by 'ezp' is set to the number
 * of zero registers. */
double hllDenseSum(uint8_t *registers, double *PE, int *ezp)
{
    double E = 0;
    int j, ez = 0;

    /* Redis default is to use 16384 registers 6 bits each. The code works
     * with other values by modifying the defines, but for our target value
     * we take a faster path with unrolled loops. */
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6)
    {
        uint8_t *r = registers;
        unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15;
        for (j = 0; j < 1024; j++)
        {
            /* Handle 16 registers per iteration. */
            r0 = r[0] & 63;
            if (r0 == 0)
                ez++;
            r1 = (r[0] >> 6 | r[1] << 2) & 63;
            if (r1 == 0)
                ez++;
            r2 = (r[1] >> 4 | r[2] << 4) & 63;
            if (r2 == 0)
                ez++;
            r3 = (r[2] >> 2) & 63;
            if (r3 == 0)
                ez++;
            r4 = r[3] & 63;
            if (r4 == 0)
                ez++;
            r5 = (r[3] >> 6 | r[4] << 2) & 63;
            if (r5 == 0)
                ez++;
            r6 = (r[4] >> 4 | r[5] << 4) & 63;
            if (r6 == 0)
                ez++;
            r7 = (r[5] >> 2) & 63;
            if (r7 == 0)
                ez++;
            r8 = r[6] & 63;
            if (r8 == 0)
                ez++;
            r9 = (r[6] >> 6 | r[7] << 2) & 63;
            if (r9 == 0)
                ez++;
            r10 = (r[7] >> 4 | r[8] << 4) & 63;
            if (r10 == 0)
                ez++;
            r11 = (r[8] >> 2) & 63;
            if (r11 == 0)
                ez++;
            r12 = r[9] & 63;
            if (r12 == 0)
                ez++;
            r13 = (r[9] >> 6 | r[10] << 2) & 63;
            if (r13 == 0)
                ez++;
            r14 = (r[10] >> 4 | r[11] << 4) & 63;
            if (r14 == 0)
                ez++;
            r15 = (r[11] >> 2) & 63;
            if (r15 == 0)
                ez++;

            /* Additional parens will allow the compiler to optimize the
             * code more with a loss of precision that is not very relevant
             * here (floating point math is not commutative!). */
            E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) + (PE[r6] + PE[r7]) + (PE[r8] + PE[r9])
                    + (PE[r10] + PE[r11]) + (PE[r12] + PE[r13]) + (PE[r14] + PE[r15]);
            r += 12;
        }
    }
    else
    {
        for (j = 0; j < HLL_REGISTERS; j++)
        {
            unsigned long reg;

            HLL_DENSE_GET_REGISTER(reg, registers, j);
            if (reg == 0)
            {
                ez++;
                /* Increment E at the end of the loop. */
            }
            else
            {
                E += PE[reg]; /* Precomputed 2^(-reg[j]). */
            }
        }
        E += ez; /* Add 2^0 'ez' times. */
    }
    *ezp = ez;
    return E;
}

/* ================== Sparse representation implementation  ================= */

/* Convert the HLL with sparse representation given as input in its dense
 * representation. Both representations are represented by SDS strings, and
 * the input representation is freed as a side effect.
 *
 * The function returns REDIS_OK if the sparse representation was valid,
 * otherwise REDIS_ERR is returned if the representation was corrupted. */
int hllSparseToDense(sds* value)
{
    sds sparse = *value;
    struct hllhdr *hdr, *oldhdr = (struct hllhdr*) sparse;
    int idx = 0, runlen, regval;
    uint8_t *p = (uint8_t*) sparse, *end = p + sdslen(sparse);

    /* If the representation is already the right one return ASAP. */
    hdr = (struct hllhdr*) sparse;
    if (hdr->encoding == HLL_DENSE)
        return 0;

    /* Create a string of the right size filled with zero bytes.
     * Note that the cached cardinality is set to 0 as a side effect
     * that is exactly the cardinality of an empty HLL. */
    //dense = sdsnewlen(NULL, HLL_DENSE_SIZE);
    sds dense = sdsnewlen(NULL, HLL_DENSE_SIZE);
    hdr = (struct hllhdr*) (&dense[0]);
    *hdr = *oldhdr; /* This will copy the magic and cached cardinality. */
    hdr->encoding = HLL_DENSE;

    /* Now read the sparse representation and set non-zero registers
     * accordingly. */
    p += HLL_HDR_SIZE;
    while (p < end)
    {
        if (HLL_SPARSE_IS_ZERO(p))
        {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            p++;
        }
        else if (HLL_SPARSE_IS_XZERO(p))
        {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            p += 2;
        }
        else
        {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            while (runlen--)
            {
                HLL_DENSE_SET_REGISTER(hdr->registers, idx, regval);
                idx++;
            }
            p++;
        }
    }

    /* If the sparse representation was valid, we expect to find idx
     * set to HLL_REGISTERS. */
    if (idx != HLL_REGISTERS)
    {
        return -1;
    }

    /* Free the old representation and set the new one. */
    sdsfree(*value);
    *value = dense;
    return 0;
}

/* "Add" the element in the sparse hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * The object 'o' is the String object holding the HLL. The function requires
 * a reference to the object in order to be able to enlarge the string if
 * needed.
 *
 * On success, the function returns 1 if the cardinality changed, or 0
 * if the register for this element was not updated.
 * On error (if the representation is invalid) -1 is returned.
 *
 * As a side effect the function may promote the HLL representation from
 * sparse to dense: this happens when a register requires to be set to a value
 * not representable with the sparse representation, or when the resulting
 * size would be greater than server.hll_sparse_max_bytes. */
int hllSparseAdd(sds* value, unsigned char *ele, size_t elesize, uint32_t hll_sparse_max_bytes)
{
    struct hllhdr *hdr;
    uint8_t oldcount, count, *sparse, *end, *p, *prev, *next;
    long index, first, span;
    long is_zero = 0, is_xzero = 0, is_val = 0, runlen = 0;
    int scanlen;

    /* Update the register if this element produced a longer run of zeroes. */
    count = hllPatLen(ele, elesize, &index);

    /* If the count is too big to be representable by the sparse representation
     * switch to dense representation. */
    if (count > HLL_SPARSE_VAL_MAX_VALUE)
        goto promote;

    /* When updating a sparse representation, sometimes we may need to
     * enlarge the buffer for up to 3 bytes in the worst case (XZERO split
     * into XZERO-VAL-XZERO). Make sure there is enough space right now
     * so that the pointers we take during the execution of the function
     * will be valid all the time. */
    *value = sdsMakeRoomFor(*value, 3);

    /* Step 1: we need to locate the opcode we need to modify to check
     * if a value update is actually needed. */
    sparse = p = ((uint8_t*) (*value)) + HLL_HDR_SIZE;
    end = p + sdslen(*value) - HLL_HDR_SIZE;

    first = 0;
    prev = NULL; /* Points to previos opcode at the end of the loop. */
    next = NULL; /* Points to the next opcode at the end of the loop. */
    span = 0;
    while (p < end)
    {
        long oplen;

        /* Set span to the number of registers covered by this opcode.
         *
         * This is the most performance critical loop of the sparse
         * representation. Sorting the conditionals from the most to the
         * least frequent opcode in many-bytes sparse HLLs is faster. */
        oplen = 1;
        if (HLL_SPARSE_IS_ZERO(p))
        {
            span = HLL_SPARSE_ZERO_LEN(p);
        }
        else if (HLL_SPARSE_IS_VAL(p))
        {
            span = HLL_SPARSE_VAL_LEN(p);
        }
        else
        { /* XZERO. */
            span = HLL_SPARSE_XZERO_LEN(p);
            oplen = 2;
        }
        /* Break if this opcode covers the register as 'index'. */
        if (index <= first + span - 1)
            break;
        prev = p;
        p += oplen;
        first += span;
    }
    if (span == 0)
        return -1; /* Invalid format. */

    next = HLL_SPARSE_IS_XZERO(p) ? p + 2 : p + 1;
    if (next >= end)
        next = NULL;

    /* Cache current opcode type to avoid using the macro again and
     * again for something that will not change.
     * Also cache the run-length of the opcode. */
    if (HLL_SPARSE_IS_ZERO(p))
    {
        is_zero = 1;
        runlen = HLL_SPARSE_ZERO_LEN(p);
    }
    else if (HLL_SPARSE_IS_XZERO(p))
    {
        is_xzero = 1;
        runlen = HLL_SPARSE_XZERO_LEN(p);
    }
    else
    {
        is_val = 1;
        runlen = HLL_SPARSE_VAL_LEN(p);
    }

    /* Step 2: After the loop:
     *
     * 'first' stores to the index of the first register covered
     *  by the current opcode, which is pointed by 'p'.
     *
     * 'next' ad 'prev' store respectively the next and previous opcode,
     *  or NULL if the opcode at 'p' is respectively the last or first.
     *
     * 'span' is set to the number of registers covered by the current
     *  opcode.
     *
     * There are different cases in order to update the data structure
     * in place without generating it from scratch:
     *
     * A) If it is a VAL opcode already set to a value >= our 'count'
     *    no update is needed, regardless of the VAL run-length field.
     *    In this case PFADD returns 0 since no changes are performed.
     *
     * B) If it is a VAL opcode with len = 1 (representing only our
     *    register) and the value is less than 'count', we just update it
     *    since this is a trivial case. */
    if (is_val)
    {
        oldcount = HLL_SPARSE_VAL_VALUE(p);
        /* Case A. */
        if (oldcount >= count)
            return 0;

        /* Case B. */
        if (runlen == 1)
        {
            HLL_SPARSE_VAL_SET(p, count, 1);
            goto updated;
        }
    }

    /* C) Another trivial to handle case is a ZERO opcode with a len of 1.
     * We can just replace it with a VAL opcode with our value and len of 1. */
    if (is_zero && runlen == 1)
    {
        HLL_SPARSE_VAL_SET(p, count, 1);
        goto updated;
    }

    /* D) General case.
     *
     * The other cases are more complex: our register requires to be updated
     * and is either currently represented by a VAL opcode with len > 1,
     * by a ZERO opcode with len > 1, or by an XZERO opcode.
     *
     * In those cases the original opcode must be split into muliple
     * opcodes. The worst case is an XZERO split in the middle resuling into
     * XZERO - VAL - XZERO, so the resulting sequence max length is
     * 5 bytes.
     *
     * We perform the split writing the new sequence into the 'new' buffer
     * with 'newlen' as length. Later the new sequence is inserted in place
     * of the old one, possibly moving what is on the right a few bytes
     * if the new sequence is longer than the older one. */
    uint8_t seq[5], *n = seq;
    int last = first + span - 1; /* Last register covered by the sequence. */
    int len;

    if (is_zero || is_xzero)
    {
        /* Handle splitting of ZERO / XZERO. */
        if (index != first)
        {
            len = index - first;
            if (len > HLL_SPARSE_ZERO_MAX_LEN)
            {
                HLL_SPARSE_XZERO_SET(n, len);
                n += 2;
            }
            else
            {
                HLL_SPARSE_ZERO_SET(n, len);
                n++;
            }
        }
        HLL_SPARSE_VAL_SET(n, count, 1);
        n++;
        if (index != last)
        {
            len = last - index;
            if (len > HLL_SPARSE_ZERO_MAX_LEN)
            {
                HLL_SPARSE_XZERO_SET(n, len);
                n += 2;
            }
            else
            {
                HLL_SPARSE_ZERO_SET(n, len);
                n++;
            }
        }
    }
    else
    {
        /* Handle splitting of VAL. */
        int curval = HLL_SPARSE_VAL_VALUE(p);

        if (index != first)
        {
            len = index - first;
            HLL_SPARSE_VAL_SET(n, curval, len);
            n++;
        }
        HLL_SPARSE_VAL_SET(n, count, 1);
        n++;
        if (index != last)
        {
            len = last - index;
            HLL_SPARSE_VAL_SET(n, curval, len);
            n++;
        }
    }

    /* Step 3: substitute the new sequence with the old one.
     *
     * Note that we already allocated space on the sds string
     * calling sdsMakeRoomFor(). */
    int seqlen = n - seq;
    int oldlen = is_xzero ? 2 : 1;
    int deltalen = seqlen - oldlen;

    if (deltalen > 0 && sdslen(*value) + deltalen > hll_sparse_max_bytes)
        goto promote;
    if (deltalen && next)
        memmove(next + deltalen, next, end - next);
    sdsIncrLen(*value, deltalen);
    memcpy(p, seq, seqlen);
    end += deltalen;

    updated:
    /* Step 4: Merge adjacent values if possible.
     *
     * The representation was updated, however the resulting representation
     * may not be optimal: adjacent VAL opcodes can sometimes be merged into
     * a single one. */
    p = prev ? prev : sparse;
    scanlen = 5; /* Scan up to 5 upcodes starting from prev. */
    while (p < end && scanlen--)
    {
        if (HLL_SPARSE_IS_XZERO(p))
        {
            p += 2;
            continue;
        }
        else if (HLL_SPARSE_IS_ZERO(p))
        {
            p++;
            continue;
        }
        /* We need two adjacent VAL opcodes to try a merge, having
         * the same value, and a len that fits the VAL opcode max len. */
        if (p + 1 < end && HLL_SPARSE_IS_VAL(p + 1))
        {
            int v1 = HLL_SPARSE_VAL_VALUE(p);
            int v2 = HLL_SPARSE_VAL_VALUE(p + 1);
            if (v1 == v2)
            {
                int len = HLL_SPARSE_VAL_LEN(p) + HLL_SPARSE_VAL_LEN(p + 1);
                if (len <= HLL_SPARSE_VAL_MAX_LEN)
                {
                    HLL_SPARSE_VAL_SET(p + 1, v1, len);
                    memmove(p, p + 1, end - p);
                    sdsIncrLen(*value, -1);
                    end--;
                    /* After a merge we reiterate without incrementing 'p'
                     * in order to try to merge the just merged value with
                     * a value on its right. */
                    continue;
                }
            }
        }
        p++;
    }

    /* Invalidate the cached cardinality. */
    hdr = (struct hllhdr *)(*value);
    HLL_INVALIDATE_CACHE(hdr);
    return 1;

    promote: /* Promote to dense representation. */
    if (hllSparseToDense(value) == -1)
        return -1; /* Corrupted HLL. */
    hdr = (struct hllhdr *)(*value);

    /* We need to call hllDenseAdd() to perform the operation after the
     * conversion. However the result must be 1, since if we need to
     * convert from sparse to dense a register requires to be updated.
     *
     * Note that this in turn means that PFADD will make sure the command
     * is propagated to slaves / AOF, so if there is a sparse -> dense
     * convertion, it will be performed in all the slaves as well. */
    int dense_retval = hllDenseAdd(hdr->registers, ele, elesize);
    ASSERT(dense_retval == 1);
    return dense_retval;
}

/* Compute SUM(2^-reg) in the sparse representation.
 * PE is an array with a pre-computer table of values 2^-reg indexed by reg.
 * As a side effect the integer pointed by 'ezp' is set to the number
 * of zero registers. */
double hllSparseSum(uint8_t *sparse, int sparselen, double *PE, int *ezp, int *invalid)
{
    double E = 0;
    int ez = 0, idx = 0, runlen, regval;
    uint8_t *end = sparse + sparselen, *p = sparse;

    while (p < end)
    {
        if (HLL_SPARSE_IS_ZERO(p))
        {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            ez += runlen;
            /* Increment E at the end of the loop. */
            p++;
        }
        else if (HLL_SPARSE_IS_XZERO(p))
        {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            ez += runlen;
            /* Increment E at the end of the loop. */
            p += 2;
        }
        else
        {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            idx += runlen;
            E += PE[regval] * runlen;
            p++;
        }
    }

    if (idx != HLL_REGISTERS && invalid)
    {
        *invalid = 1;
    }
    E += ez; /* Add 2^0 'ez' times. */
    *ezp = ez;
    return E;
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level hllDenseSum() and hllSparseSum() functions
 * as helpers to compute the SUM(2^-reg) part of the computation, which is
 * representation-specific, while all the rest is common. */

/* Implements the SUM operation for uint8_t data type which is only used
 * internally as speedup for PFCOUNT with multiple keys. */
double hllRawSum(uint8_t *registers, double *PE, int *ezp)
{
    double E = 0;
    int j, ez = 0;
    uint64_t *word = (uint64_t*) registers;
    uint8_t *bytes;

    for (j = 0; j < HLL_REGISTERS / 8; j++)
    {
        if (*word == 0)
        {
            ez += 8;
        }
        else
        {
            bytes = (uint8_t*) word;
            if (bytes[0])
                E += PE[bytes[0]];
            else
                ez++;
            if (bytes[1])
                E += PE[bytes[1]];
            else
                ez++;
            if (bytes[2])
                E += PE[bytes[2]];
            else
                ez++;
            if (bytes[3])
                E += PE[bytes[3]];
            else
                ez++;
            if (bytes[4])
                E += PE[bytes[4]];
            else
                ez++;
            if (bytes[5])
                E += PE[bytes[5]];
            else
                ez++;
            if (bytes[6])
                E += PE[bytes[6]];
            else
                ez++;
            if (bytes[7])
                E += PE[bytes[7]];
            else
                ez++;
        }
        word++;
    }
    E += ez; /* 2^(-reg[j]) is 1 when m is 0, add it 'ez' times for every
     zero register in the HLL. */
    *ezp = ez;
    return E;
}

/* Return the approximated cardinality of the set based on the armonic
 * mean of the registers values. 'hdr' points to the start of the SDS
 * representing the String object holding the HLL representation.
 *
 * If the sparse representation of the HLL object is not valid, the integer
 * pointed by 'invalid' is set to non-zero, otherwise it is left untouched.
 *
 * hllCount() supports a special internal-only encoding of HLL_RAW, that
 * is, hdr->registers will point to an uint8_t array of HLL_REGISTERS element.
 * This is useful in order to speedup PFCOUNT when called against multiple
 * keys (no need to work with 6-bit integers encoding). */
uint64_t hllCount(struct hllhdr *hdr, uint32_t slen, int *invalid)
{
    double m = HLL_REGISTERS;
    double E, alpha = 0.7213 / (1 + 1.079 / m);
    int j, ez; /* Number of registers equal to 0. */

    /* We precompute 2^(-reg[j]) in a small table in order to
     * speedup the computation of SUM(2^-register[0..i]). */
    static int initialized = 0;
    static double PE[64];
    if (!initialized)
    {
        PE[0] = 1; /* 2^(-reg[j]) is 1 when m is 0. */
        for (j = 1; j < 64; j++)
        {
            /* 2^(-reg[j]) is the same as 1/2^reg[j]. */
            PE[j] = 1.0 / (1ULL << j);
        }
        initialized = 1;
    }

    /* Compute SUM(2^-register[0..i]). */
    if (hdr->encoding == HLL_DENSE)
    {
        E = hllDenseSum(hdr->registers, PE, &ez);
    }
    else if (hdr->encoding == HLL_SPARSE)
    {
        E = hllSparseSum(hdr->registers, slen - HLL_HDR_SIZE, PE, &ez, invalid);
    }
    else if (hdr->encoding == HLL_RAW)
    {
        E = hllRawSum(hdr->registers, PE, &ez);
    }
    else
    {
        //redisPanic("Unknown HyperLogLog encoding in hllCount()");
        abort();
    }

    /* Muliply the inverse of E for alpha_m * m^2 to have the raw estimate. */
    E = (1 / E) * alpha * m * m;

    /* Use the LINEARCOUNTING algorithm for small cardinalities.
     * For larger values but up to 72000 HyperLogLog raw approximation is
     * used since linear counting error starts to increase. However HyperLogLog
     * shows a strong bias in the range 2.5*16384 - 72000, so we try to
     * compensate for it. */
    if (E < m * 2.5 && ez != 0)
    {
        E = m * log(m / ez); /* LINEARCOUNTING() */
    }
    else if (m == 16384 && E < 72000)
    {
        /* We did polynomial regression of the bias for this range, this
         * way we can compute the bias for a given cardinality and correct
         * according to it. Only apply the correction for P=14 that's what
         * we use and the value the correction was verified with. */
        double bias = 5.9119 * 1.0e-18 * (E * E * E * E) - 1.4253 * 1.0e-12 * (E * E * E) + 1.2940 * 1.0e-7 * (E * E)
                - 5.2921 * 1.0e-3 * E + 83.3216;
        E -= E * (bias / 100);
    }
    /* We don't apply the correction for E > 1/30 of 2^32 since we use
     * a 64 bit function and 6 bit counters. To apply the correction for
     * 1/30 of 2^64 is not needed since it would require a huge set
     * to approach such a value. */
    return (uint64_t) E;
}

/* Call hllDenseAdd() or hllSparseAdd() according to the HLL encoding. */
int hllAdd(std::string& value, unsigned char *ele, size_t elesize, uint32_t hll_sparse_max_bytes)
{
    struct hllhdr *hdr = (struct hllhdr *) (&value[0]);
    switch (hdr->encoding)
    {
        case HLL_DENSE:
            return hllDenseAdd(hdr->registers, ele, elesize);
        case HLL_SPARSE:
        {
            sds v = sdsnewlen(value.data(), value.size());
            int retval = hllSparseAdd(&v, ele, elesize, hll_sparse_max_bytes);
            if (retval > 0)
            {
                value.clear();
                value.append(v, sdslen(v));
                sdsfree(v);
            }
            return retval;
        }
        default:
            return -1; /* Invalid representation. */
    }
}

/* Merge by computing MAX(registers[i],hll[i]) the HyperLogLog 'hll'
 * with an array of uint8_t HLL_REGISTERS registers pointed by 'max'.
 *
 * The hll object must be already validated via isHLLObjectOrReply()
 * or in some other way.
 *
 * If the HyperLogLog is sparse and is found to be invalid, REDIS_ERR
 * is returned, otherwise the function always succeeds. */
int hllMerge(uint8_t *max, std::string& hll)
{
    struct hllhdr *hdr = (struct hllhdr *) (&hll[0]);
    int i;

    if (hdr->encoding == HLL_DENSE)
    {
        uint8_t val;

        for (i = 0; i < HLL_REGISTERS; i++)
        {
            HLL_DENSE_GET_REGISTER(val, hdr->registers, i);
            if (val > max[i])
                max[i] = val;
        }
    }
    else
    {
        uint8_t *p = (uint8_t *) (&hll[0]), *end = p + hll.size();
        long runlen, regval;

        p += HLL_HDR_SIZE;
        i = 0;
        while (p < end)
        {
            if (HLL_SPARSE_IS_ZERO(p))
            {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                i += runlen;
                p++;
            }
            else if (HLL_SPARSE_IS_XZERO(p))
            {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                i += runlen;
                p += 2;
            }
            else
            {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                while (runlen--)
                {
                    if (regval > max[i])
                        max[i] = regval;
                    i++;
                }
                p++;
            }
        }
        if (i != HLL_REGISTERS)
            return -1;
    }
    return 0;
}

/* ========================== HyperLogLog commands ========================== */

/* Create an HLL object. We always create the HLL using sparse encoding.
 * This will be upgraded to the dense representation as needed. */
void createHLLObject(std::string& value)
{
    struct hllhdr *hdr;
    uint8_t *p;
    int sparselen = HLL_HDR_SIZE + (((HLL_REGISTERS + (HLL_SPARSE_XZERO_MAX_LEN - 1)) /
    HLL_SPARSE_XZERO_MAX_LEN) * 2);
    int aux;

    /* Populate the sparse representation with as many XZERO opcodes as
     * needed to represent all the registers. */
    aux = HLL_REGISTERS;
    //s = sdsnewlen(NULL, sparselen);
    value.resize(sparselen);
    p = (uint8_t*) (&value[0]) + HLL_HDR_SIZE;
    while (aux)
    {
        int xzero = HLL_SPARSE_XZERO_MAX_LEN;
        if (xzero > aux)
            xzero = aux;
        HLL_SPARSE_XZERO_SET(p, xzero);
        p += 2;
        aux -= xzero;
    }
    //redisAssert((p - (uint8_t*) s) == sparselen);

    /* Create the actual object. */
    //o = createObject(REDIS_STRING, s);
    hdr = (struct hllhdr *) (&value[0]);
    memcpy(hdr->magic, "HYLL", 4);
    hdr->encoding = HLL_SPARSE;
}

/* Check if the object is a String with a valid HLL representation.
 * Return REDIS_OK if this is true, otherwise reply to the client
 * with an error and return REDIS_ERR. */
bool isHLLObjectOrReply(std::string& value)
{
    struct hllhdr *hdr;
    if (value.size() < sizeof(*hdr))
        goto invalid;
    hdr = (struct hllhdr *) (&value[0]);

    /* Magic should be "HYLL". */
    if (hdr->magic[0] != 'H' || hdr->magic[1] != 'Y' || hdr->magic[2] != 'L' || hdr->magic[3] != 'L')
        goto invalid;

    if (hdr->encoding > HLL_MAX_ENCODING)
        goto invalid;

    /* Dense representation string length should match exactly. */
    if (hdr->encoding == HLL_DENSE && value.size() != HLL_DENSE_SIZE)
        goto invalid;

    /* All tests passed. */
    return true;
    invalid:
//    invalid: addReplySds(c, sdsnew("-WRONGTYPE Key is not a valid "
//            "HyperLogLog string value.\r\n"));
    return false;
}

namespace ardb
{

    int Ardb::PFAdd(const DBID& db, const Slice& key, const SliceArray& members)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        std::string value;
        int updated = 0;
        int ret = Get(db, key, value);
        if (0 == ret)
        {
            if (!isHLLObjectOrReply(value))
            {
                return ERR_INVALID_HLL_TYPE;
            }
        }
        else if (ERR_NOT_EXIST == ret)
        {
            createHLLObject(value);
            updated++;
        }
        else
        {
            return ERR_INVALID_TYPE;
        }
        for (uint32 i = 0; i < members.size(); i++)
        {
            int retval = hllAdd(value, (unsigned char*) (members[i].data()), members[i].size(),
                    (uint32_t) m_config.hll_sparse_max_bytes);
            switch (retval)
            {
                case 1:
                    updated++;
                    break;
                case -1:
                    //addReplySds(c, sdsnew(invalid_hll_err));
                    return ERR_CORRUPTED_HLL_VALUE;
            }
        }
        struct hllhdr* hdr = (struct hllhdr*) (&value[0]);
        if (updated)
        {
            HLL_INVALIDATE_CACHE(hdr);
            int ret = Set(db, key, value);
            return ret == 0 ? 1 : ret;
        }
        return 0;
    }

    int Ardb::PFCountKey(const DBID& db, const Slice& key, uint64& v)
    {
        KeyLockerGuard keyguard(m_key_locker, db, key);
        std::string value;
        int ret = Get(db, key, value);
        if (0 == ret)
        {
            if (!isHLLObjectOrReply(value))
            {
                return ERR_INVALID_HLL_TYPE;
            }
        }
        else if (ERR_NOT_EXIST == ret)
        {
            return 0;
        }
        else
        {
            return ret;
        }
        struct hllhdr *hdr;
        uint64_t card = 0;

        /* Check if the cached cardinality is valid. */
        hdr = (struct hllhdr *) (&value[0]);
        if (HLL_VALID_CACHE(hdr))
        {
            /* Just return the cached value. */
            card = (uint64_t) hdr->card[0];
            card |= (uint64_t) hdr->card[1] << 8;
            card |= (uint64_t) hdr->card[2] << 16;
            card |= (uint64_t) hdr->card[3] << 24;
            card |= (uint64_t) hdr->card[4] << 32;
            card |= (uint64_t) hdr->card[5] << 40;
            card |= (uint64_t) hdr->card[6] << 48;
            card |= (uint64_t) hdr->card[7] << 56;
        }
        else
        {
            int invalid = 0;
            /* Recompute it and update the cached value. */
            card = hllCount(hdr, value.size(), &invalid);
            if (invalid)
            {
                //addReplySds(c, sdsnew(invalid_hll_err));
                return ERR_CORRUPTED_HLL_VALUE;
            }
            hdr->card[0] = card & 0xff;
            hdr->card[1] = (card >> 8) & 0xff;
            hdr->card[2] = (card >> 16) & 0xff;
            hdr->card[3] = (card >> 24) & 0xff;
            hdr->card[4] = (card >> 32) & 0xff;
            hdr->card[5] = (card >> 40) & 0xff;
            hdr->card[6] = (card >> 48) & 0xff;
            hdr->card[7] = (card >> 56) & 0xff;
            /* This is not considered a read-only command even if the
             * data structure is not modified, since the cached value
             * may be modified and given that the HLL is a Redis string
             * we need to propagate the change. */
            //signalModifiedKey(c->db, c->argv[1]);
            //server.dirty++;
            Set(db, key, value);
        }
        v = card;
        return 0;
    }

    /* PFCOUNT var -> approximated cardinality of set. */
    int Ardb::PFCount(const DBID& db, const SliceArray& keys, uint64& card)
    {
        if (keys.size() == 1)
        {
            return PFCountKey(db, keys[0], card);
        }
        uint8_t max[HLL_HDR_SIZE + HLL_REGISTERS], *registers;

        /* Compute an HLL with M[i] = MAX(M[i]_j). */
        memset(max, 0, sizeof(max));
        struct hllhdr *hdr = (struct hllhdr*) max;
        hdr->encoding = HLL_RAW; /* Special internal-only encoding. */
        registers = max + HLL_HDR_SIZE;
        for (uint32 i = 0; i < keys.size(); i++)
        {
            std::string hll;
            int ret = Get(db, keys[i], hll);
            if (0 == ret)
            {
                if (!isHLLObjectOrReply(hll))
                {
                    return ERR_INVALID_HLL_TYPE;
                }
            }
            else if (ERR_NOT_EXIST == ret)
            {
                continue;
            }
            else
            {
                return ret;
            }

            /* Merge with this HLL with our 'max' HHL by setting max[i]
             * to MAX(max[i],hll[i]). */
            if (hllMerge(registers, hll) == -1)
            {
                return ERR_CORRUPTED_HLL_VALUE;
            }
        }
        card = hllCount(hdr, sizeof(max), NULL);
        return 0;
    }

    int Ardb::PFMerge(const DBID& db, const Slice& destkey, const SliceArray& srckeys)
    {
        uint8_t max[HLL_REGISTERS];
        struct hllhdr *hdr;
        int j;

        /* Compute an HLL with M[i] = MAX(M[i]_j).
         * We we the maximum into the max array of registers. We'll write
         * it to the target variable later. */
        memset(max, 0, sizeof(max));
        for (uint32 j = 1; j < srckeys.size(); j++)
        {
            std::string hll;
            int ret = Get(db, srckeys[j], hll);
            if (0 == ret)
            {
                if (!isHLLObjectOrReply(hll))
                {
                    return ERR_INVALID_HLL_TYPE;
                }
            }
            else if (ERR_NOT_EXIST == ret)
            {
                continue;
            }
            else
            {
                return ret;
            }

            /* Merge with this HLL with our 'max' HHL by setting max[i]
             * to MAX(max[i],hll[i]). */
            if (hllMerge(max, hll) == -1)
            {
                return ERR_CORRUPTED_HLL_VALUE;
            }
        }

        /* Create / unshare the destination key's value if needed. */
        std::string hll;
        int ret = Get(db, destkey, hll);
        if (0 == ret)
        {
            if (!isHLLObjectOrReply(hll))
            {
                return ERR_INVALID_HLL_TYPE;
            }
        }
        else if (ERR_NOT_EXIST == ret)
        {
            createHLLObject(hll);
        }
        else
        {
            return ret;
        }

        /* Only support dense objects as destination. */
        sds hlls = sdsnewlen(hll.data(), hll.size());
        if (hllSparseToDense(&hlls) == -1)
        {
            sdsfree(hlls);
            return ERR_CORRUPTED_HLL_VALUE;
        }

        /* Write the resulting HLL to the destination HLL registers and
         * invalidate the cached value. */
        hdr = (struct hllhdr *) hlls;
        for (j = 0; j < HLL_REGISTERS; j++)
        {
            HLL_DENSE_SET_REGISTER(hdr->registers, j, max[j]);
        }
        HLL_INVALIDATE_CACHE(hdr);
        hll.clear();
        hll.append(hlls, sdslen(hlls));
        return Set(db, destkey, hll);
    }

    /* PFADD var ele ele ele ... ele => :0 or :1 */
    int ArdbServer::PFAdd(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        SliceArray members;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            members.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_db->PFAdd(ctx.currentDB, key, members);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        if (ret >= 0)
        {
            fill_int_reply(ctx.reply, ret);
            return 0;
        }
        else
        {
            fill_error_reply(ctx.reply, "Operation against a key holding the wrong kind of value");
        }
        return 0;

    }
    /* PFCOUNT var -> approximated cardinality of set. */
    int ArdbServer::PFCount(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        uint64 card = 0;
        SliceArray keys;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_db->PFCount(ctx.currentDB, keys, card);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        fill_int_reply(ctx.reply, (int64) card);
        return 0;
    }

    int ArdbServer::PFMerge(ArdbConnContext& ctx, RedisCommandFrame& cmd)
    {
        SliceArray srckeys;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            srckeys.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_db->PFMerge(ctx.currentDB, cmd.GetArguments()[0], srckeys);
        CHECK_ARDB_RETURN_VALUE(ctx.reply, ret);
        if (0 == ret)
        {
            fill_status_reply(ctx.reply, "OK");
        }
        else
        {
            fill_error_reply(ctx.reply, "Operation against a key holding the wrong kind of value");
        }
        return 0;
    }

}
