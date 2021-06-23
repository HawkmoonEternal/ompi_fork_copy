# -----
# This file is auto-generated during autogen.pl
# -----

AC_DEFUN([OMPI_PMIX_ADD_ARGS],[


# ----------------------------------------

# Warning: Excluded: pkgconfigdir
# ---------------------------------------- Above from 3rd-party/openpmix//config/pkg.m4:173

# Warning: Excluded: noarch-pkgconfigdir
# ---------------------------------------- Above from 3rd-party/openpmix//config/pkg.m4:193

    AC_ARG_WITH([pmix-extra-lib],
                AS_HELP_STRING([--with-pmix-extra-lib=LIB],
                               [Link the output PMIx library to this extra lib (used in embedded mode)]))
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:202

    AC_ARG_WITH([pmix-extra-ltlib],
                AS_HELP_STRING([--with-pmix-extra-ltlib=LIB],
                               [Link any embedded components/tools that require it to the provided libtool lib (used in embedded mode)]))
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:220

    AC_ARG_WITH([pmix-package-string],
         [AS_HELP_STRING([--with-pmix-package-string=STRING],
                         [Use a branding string throughout PMIx])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:241

# Warning: Excluded: show-load-errors-by-default
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:833

# Warning: Excluded: dlopen
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:997

    AC_ARG_ENABLE([embedded-mode],
        [AS_HELP_STRING([--enable-embedded-mode],
                [Using --enable-embedded-mode causes PMIx to skip a few configure checks and install nothing.  It should only be used when building PMIx within the scope of a larger package.])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1018

# Warning: Excluded: picky
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1051

# Warning: Excluded: debug
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1073

# Warning: Excluded: debug-symbols
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1091

# Warning: Excluded: devel-headers
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1099

AC_ARG_WITH([tests-examples],
    [AS_HELP_STRING([--with-tests-examples],
            [Whether or not to install the tests and example programs.])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1115

# Warning: Excluded: per-user-config-files
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1134

# Warning: Excluded: pretty-print-stacktrace
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1150

AC_ARG_ENABLE([dstore-pthlck],
              [AS_HELP_STRING([--disable-dstore-pthlck],
                              [Disable pthread-based locking in dstor (default: enabled)])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1171

# Warning: Excluded: ident-string
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1184

AC_ARG_ENABLE(pmix-timing,
              AS_HELP_STRING([--enable-pmix-timing],
                             [enable PMIx developer-level timing code (default: disabled)]))
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1210

AC_ARG_ENABLE(pmix-binaries,
              AS_HELP_STRING([--enable-pmix-binaries],
                             [enable PMIx tools]))
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1230

AC_ARG_ENABLE(python-bindings,
              AS_HELP_STRING([--enable-python-bindings],
                             [enable Python bindings (default: disabled)]))
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1247

AC_ARG_ENABLE([nonglobal-dlopen],
              AS_HELP_STRING([--enable-nonglobal-dlopen],
                             [enable non-global dlopen (default: enabled)]))
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1321

# Warning: Excluded: pty-support
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1340

AC_ARG_ENABLE(dummy-handshake,
              AS_HELP_STRING([--enable-dummy-handshake],
                             [Enables psec dummy component intended to check the PTL handshake scenario (default: disabled)]))
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix.m4:1360

# Warning: Excluded: alps-libdir
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_alps.m4:41

# Warning: Excluded: alps
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_alps.m4:92

# Warning: Excluded: broken-qsort
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_broken_qsort.m4:41

	AC_ARG_WITH([curl],
		    [AS_HELP_STRING([--with-curl(=DIR)],
				    [Build curl support (default=no), optionally adding DIR/include, DIR/lib, and DIR/lib64 to the search path for headers and libraries])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_curl.m4:37

    AC_ARG_WITH([curl-libdir],
            [AS_HELP_STRING([--with-curl-libdir=DIR],
                    [Search for Curl libraries in DIR])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_curl.m4:42

	AC_ARG_WITH([jansson],
		    [AS_HELP_STRING([--with-jansson(=DIR)],
				    [Build jansson support (default=no), optionally adding DIR/include, DIR/lib, and DIR/lib64 to the search path for headers and libraries])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_jansson.m4:37

    AC_ARG_WITH([jansson-libdir],
            [AS_HELP_STRING([--with-jansson-libdir=DIR],
                    [Search for Jansson libraries in DIR])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_jansson.m4:42

# Warning: Excluded: lustre
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_lustre.m4:46

# Warning: Excluded: libfabric
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_ofi.m4:66

# Warning: Excluded: libfabric-libdir
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_ofi.m4:69

# Warning: Excluded: ofi
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_ofi.m4:73

# Warning: Excluded: ofi-libdir
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_ofi.m4:77

# Warning: Excluded: psm2
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_psm2.m4:38

# Warning: Excluded: psm2-libdir
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_psm2.m4:42

	AC_ARG_WITH([slurm],
           [AS_HELP_STRING([--with-slurm],
                           [Build SLURM scheduler component (default: yes)])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_slurm.m4:31

	AC_ARG_WITH([tm],
                    [AS_HELP_STRING([--with-tm(=DIR)],
                                    [Build TM (Torque, PBSPro, and compatible) support, optionally adding DIR/include, DIR/lib, and DIR/lib64 to the search path for headers and libraries])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_tm.m4:55

# Warning: Excluded: visibility
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_check_visibility.m4:31

# Warning: Excluded: c11-atomics
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_config_asm.m4:84

# Warning: Excluded: builtin-atomics
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_config_asm.m4:86

# Warning: Excluded: max-
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_functions.m4:594

    AC_ARG_WITH([pmix-platform-patches-dir],
        [AS_HELP_STRING([--with-pmix-platform-patches-dir=DIR],
                        [Location of the platform patches directory. If you use this option, you must also use --with-platform.])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_load_platform.m4:29

    AC_ARG_WITH([pmix-platform],
        [AS_HELP_STRING([--with-pmix-platform=FILE],
                        [Load options for build from FILE.  Options on the
                         command line not in FILE are used.  Options on the
                         command line and in FILE are replaced by what is
                         in FILE.])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_load_platform.m4:36

# Warning: Excluded: mca-no-build
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_mca.m4:55

# Warning: Excluded: mca-dso
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_mca.m4:59

# Warning: Excluded: mca-static
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_mca.m4:67

    AC_ARG_WITH([libev],
                [AS_HELP_STRING([--with-libev=DIR],
                                [Search for libev headers and libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_libev.m4:23

    AC_ARG_WITH([libev-libdir],
                [AS_HELP_STRING([--with-libev-libdir=DIR],
                                [Search for libev libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_libev.m4:28

# Warning: Excluded: libevent
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_libevent.m4:46

# Warning: Excluded: libevent-libdir
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_libevent.m4:50

# Warning: Excluded: libevent-header
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_libevent.m4:54

# Warning: Excluded: man-pages
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_man_pages.m4:22

# Warning: Excluded: wrapper-cflags
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_wrappers.m4:78

# Warning: Excluded: wrapper-cflags-prefix
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_wrappers.m4:84

# Warning: Excluded: wrapper-ldflags
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_wrappers.m4:90

# Warning: Excluded: wrapper-libs
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_wrappers.m4:96

# Warning: Excluded: wrapper-rpath
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_wrappers.m4:103

# Warning: Excluded: wrapper-runpath
# ---------------------------------------- Above from 3rd-party/openpmix//config/pmix_setup_wrappers.m4:110

AC_ARG_ENABLE(werror,
    AS_HELP_STRING([--enable-werror],
                   [Treat compiler warnings as errors]))
# ---------------------------------------- Above from 3rd-party/openpmix//configure.ac:337

    AC_ARG_WITH([zlib],
                [AS_HELP_STRING([--with-zlib=DIR],
                                [Search for zlib headers and libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pcompress/zlib/configure.m4:24

    AC_ARG_WITH([zlib-libdir],
                [AS_HELP_STRING([--with-zlib-libdir=DIR],
                                [Search for zlib libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pcompress/zlib/configure.m4:28

# Warning: Excluded: dl-dlopen
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pdl/pdlopen/configure.m4:47

    AC_ARG_WITH([plibltdl],
        [AS_HELP_STRING([--with-libltdl(=DIR)],
             [Build libltdl support, optionally adding DIR/include, DIR/lib, and DIR/lib64 to the search path for headers and libraries])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pdl/plibltdl/configure.m4:48

# Warning: Excluded: libltdl-libdir
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pdl/plibltdl/configure.m4:49

    AC_ARG_WITH([hwloc],
                [AS_HELP_STRING([--with-hwloc=DIR],
                                [Search for hwloc headers and libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/ploc/hwloc/configure.m4:41

    AC_ARG_WITH([hwloc-libdir],
                [AS_HELP_STRING([--with-hwloc-libdir=DIR],
                                [Search for hwloc libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/ploc/hwloc/configure.m4:45

    AC_ARG_WITH([simptest], [AS_HELP_STRING([--with-simptest], [Include simptest fabric support])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pnet/simptest/configure.m4:18

    AC_ARG_WITH([slingshot], [AS_HELP_STRING([--with-slingshot], [Include Slingshot fabric support])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pnet/sshot/configure.m4:18

    AC_ARG_WITH([cxi], [AS_HELP_STRING([--with-cxi(=DIR)],
                                       [Include CXI service library support, optionally adding DIR/include, DIR/include/cxi, DIR/lib, and DIR/lib64 to the search path for headers and libraries])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pnet/sshot/configure.m4:22

    AC_ARG_WITH([cxi-libdir],
                [AS_HELP_STRING([--with-cxi-libdir=DIR],
                                [Search for CXI libraries in DIR])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/pnet/sshot/configure.m4:27

    AC_ARG_WITH([munge],
                [AS_HELP_STRING([--with-munge=DIR],
                                [Search for munge headers and libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/psec/munge/configure.m4:23

    AC_ARG_WITH([munge-libdir],
                [AS_HELP_STRING([--with-munge-libdir=DIR],
                                [Search for munge libraries in DIR ])])
# ---------------------------------------- Above from 3rd-party/openpmix//src/mca/psec/munge/configure.m4:27


])
