/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2010      ARM ltd.  All rights reserved.
 * Copyright (c) 2016-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2021      Triad National Security, LLC. All rights reserved.
 * Copyright (c) 2021      Google, LLC. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OPAL_SYS_ARCH_ATOMIC_LLSC_H)

#    define OPAL_SYS_ARCH_ATOMIC_LLSC_H

#    if OPAL_C_GCC_INLINE_ASSEMBLY

#        undef OPAL_HAVE_ATOMIC_LLSC_32
#        undef OPAL_HAVE_ATOMIC_LLSC_64

#        define OPAL_HAVE_ATOMIC_LLSC_32 1
#        define OPAL_HAVE_ATOMIC_LLSC_64 1

#        define opal_atomic_ll_32(addr, ret)                                                       \
            do {                                                                                   \
                opal_atomic_int32_t *_addr = (addr);                                               \
                int32_t _ret;                                                                      \
                                                                                                   \
                __asm__ __volatile__("ldaxr    %w0, [%1]          \n" : "=&r"(_ret) : "r"(_addr)); \
                                                                                                   \
                ret = (typeof(ret)) _ret;                                                          \
            } while (0)

#        define opal_atomic_sc_32(addr, newval, ret)                  \
            do {                                                      \
                opal_atomic_int32_t *_addr = (addr);                  \
                int32_t _newval = (int32_t) newval;                   \
                int _ret;                                             \
                                                                      \
                __asm__ __volatile__("stlxr    %w0, %w2, [%1]     \n" \
                                     : "=&r"(_ret)                    \
                                     : "r"(_addr), "r"(_newval)       \
                                     : "cc", "memory");               \
                                                                      \
                ret = (_ret == 0);                                    \
            } while (0)

#        define opal_atomic_ll_64(addr, ret)                                                      \
            do {                                                                                  \
                opal_atomic_int64_t *_addr = (addr);                                              \
                int64_t _ret;                                                                     \
                                                                                                  \
                __asm__ __volatile__("ldaxr    %0, [%1]          \n" : "=&r"(_ret) : "r"(_addr)); \
                                                                                                  \
                ret = (typeof(ret)) _ret;                                                         \
            } while (0)

#        define opal_atomic_sc_64(addr, newval, ret)                 \
            do {                                                     \
                opal_atomic_int64_t *_addr = (addr);                 \
                int64_t _newval = (int64_t) newval;                  \
                int _ret;                                            \
                                                                     \
                __asm__ __volatile__("stlxr    %w0, %2, [%1]     \n" \
                                     : "=&r"(_ret)                   \
                                     : "r"(_addr), "r"(_newval)      \
                                     : "cc", "memory");              \
                                                                     \
                ret = (_ret == 0);                                   \
            } while (0)

#    endif /* OPAL_GCC_INLINE_ASSEMBLY */

#endif /* ! OPAL_SYS_ARCH_ATOMIC_LLSC_H */
