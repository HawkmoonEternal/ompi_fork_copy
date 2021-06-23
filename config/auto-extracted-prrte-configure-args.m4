# -----
# This file is auto-generated during autogen.pl
# -----

AC_DEFUN([OMPI_PRRTE_ADD_ARGS],[


# ----------------------------------------

# Warning: Excluded: pkgconfigdir
# ---------------------------------------- Above from 3rd-party/prrte//config/pkg.m4:173

# Warning: Excluded: noarch-pkgconfigdir
# ---------------------------------------- Above from 3rd-party/prrte//config/pkg.m4:193

# Warning: Excluded: cs_fs
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_case_sensitive_fs_setup.m4:65

# Warning: Excluded: alps-libdir
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_alps.m4:41

# Warning: Excluded: alps
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_alps.m4:92

# Warning: Excluded: broken-qsort
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_broken_qsort.m4:41

# Warning: Excluded: libnl
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_libnl.m4:42

       AC_ARG_WITH([lsf],
               [AS_HELP_STRING([--with-lsf(=DIR)],
                       [Build LSF support])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_lsf.m4:35

       AC_ARG_WITH([lsf-libdir],
               [AS_HELP_STRING([--with-lsf-libdir=DIR],
                       [Search for LSF libraries in DIR])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_lsf.m4:39

        AC_ARG_WITH([moab],
                    [AS_HELP_STRING([--with-moab],
                                    [Build MOAB scheduler component (default: yes)])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_moab.m4:33

        AC_ARG_WITH([moab-libdir],
                    [AS_HELP_STRING([--with-moab-libdir=DIR],
                    [Search for Moab libraries in DIR])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_moab.m4:37

	AC_ARG_WITH([sge],
                    [AS_HELP_STRING([--with-sge],
                                    [Build SGE or Grid Engine support (default: no)])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_sge.m4:36

    AC_ARG_WITH([singularity],
                [AS_HELP_STRING([--with-singularity(=DIR)],
                                [Build support for the Singularity container, optionally adding DIR to the search path])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_singularity.m4:20

# Warning: Excluded: slurm
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_slurm.m4:29

# Warning: Excluded: tm
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_tm.m4:53

# Warning: Excluded: visibility
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_check_visibility.m4:31

# Warning: Excluded: c11-atomics
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_config_asm.m4:85

# Warning: Excluded: builtin-atomics
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_config_asm.m4:87

# Warning: Excluded: prte-prefix-by-default
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:37

# Warning: Excluded: picky
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:66

# Warning: Excluded: debug
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:87

# Warning: Excluded: debug-symbols
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:106

# Warning: Excluded: devel-headers
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:114

# Warning: Excluded: pretty-print-stacktrace
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:131

# Warning: Excluded: pty-support
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:151

# Warning: Excluded: dlopen
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:170

# Warning: Excluded: show-load-errors-by-default
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:192

AC_ARG_WITH(proxy-version-string,
    AS_HELP_STRING([--with-proxy-version-string],
                   [Return the provided string when prte is used in proxy mode and the version is requested]))
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:224

AC_ARG_WITH(proxy-package-name,
    AS_HELP_STRING([--with-proxy-package-name],
                   [Return the provided string when prte is used in proxy mode and the package name is requested]))
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:251

AC_ARG_WITH(proxy-bugreport,
    AS_HELP_STRING([--with-proxy-bugreport],
                   [Return the provided string when prte is used in proxy mode and the PACKAGE_BUGREPORT is requested]))
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:265

# Warning: Excluded: per-user-config-files
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:280

# Warning: Excluded: ipv6
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:295

AC_ARG_WITH([prte-extra-lib],
            AS_HELP_STRING([--with-prte-extra-lib=LIB],
                           [Link the output PRTE library to this extra lib (used in embedded mode)]))
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:312

AC_ARG_WITH([prte-extra-ltlib],
            AS_HELP_STRING([--with-prte-extra-ltlib=LIB],
                           [Link any embedded components/tools that require it to the provided libtool lib (used in embedded mode)]))
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:330

AC_ARG_WITH([prte-extra-lib-ldflags],
            AS_HELP_STRING([--with-prte-extra-lib-ldflags=flags],
                           [Where to find the extra libs]))
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:348

# Warning: Excluded: package-string
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:367

# Warning: Excluded: ident-string
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:381

# Warning: Excluded: getpwuid
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:404

AC_ARG_ENABLE([prte-ft],
    [AS_HELP_STRING([--enable-prte-ft],
        [ENable PRRTE fault tolerance support (default: disabled)])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_configure_options.m4:421

# Warning: Excluded: max-
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_functions.m4:599

    AC_ARG_WITH([prte-platform-patches-dir],
        [AS_HELP_STRING([--with-prte-platform-patches-dir=DIR],
                        [Location of the platform patches directory. If you use this option, you must also use --with-platform.])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_load_platform.m4:30

    AC_ARG_WITH([prte-platform],
        [AS_HELP_STRING([--with-prte-platform=FILE],
                        [Load options for build from FILE.  Options on the
                         command line not in FILE are used.  Options on the
                         command line and in FILE are replaced by what is
                         in FILE.])])
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_load_platform.m4:37

# Warning: Excluded: mca-no-build
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_mca.m4:55

# Warning: Excluded: mca-dso
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_mca.m4:59

# Warning: Excluded: mca-static
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_mca.m4:67

# Warning: Skipped (embedded variable): $3
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_component_package.m4:63

# Warning: Skipped (embedded variable): $3-libdir
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_component_package.m4:67

# Warning: Excluded: hwloc
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_hwloc.m4:19

# Warning: Excluded: hwloc-libdir
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_hwloc.m4:23

# Warning: Excluded: hwloc-header
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_hwloc.m4:27

# Warning: Excluded: libev
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_libev.m4:22

# Warning: Excluded: libev-libdir
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_libev.m4:26

# Warning: Excluded: libevent
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_libevent.m4:22

# Warning: Excluded: libevent-header
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_libevent.m4:25

# Warning: Excluded: libevent-libdir
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_libevent.m4:29

# Warning: Excluded: man-pages
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_man_pages.m4:22

# Warning: Excluded: pmix
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_pmix.m4:32

# Warning: Excluded: pmix-libdir
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_pmix.m4:36

# Warning: Excluded: pmix-header
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_pmix.m4:40

# Warning: Excluded: pmix-devel-support
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_pmix.m4:44

# Warning: Excluded: wrapper-cflags
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:78

# Warning: Excluded: wrapper-cflags-prefix
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:84

# Warning: Excluded: wrapper-cxxflags
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:90

# Warning: Excluded: wrapper-cxxflags-prefix
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:96

# Warning: Excluded: wrapper-ldflags
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:102

# Warning: Excluded: wrapper-libs
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:108

# Warning: Excluded: wrapper-rpath
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:115

# Warning: Excluded: wrapper-runpath
# ---------------------------------------- Above from 3rd-party/prrte//config/prte_setup_wrappers.m4:122

    AC_ARG_ENABLE([prtedl-dlopen],
        [AS_HELP_STRING([--disable-prtedl-dlopen],
            [Disable the "dlopen" PRTE DL component (and probably force the use of the "libltdl" DL component).  This option should really only be used by PRTE developers.  You are probably actually looking for the "--disable-prtedlopen" option, which disables all dlopen-like functionality from PRTE.])
        ])
# ---------------------------------------- Above from 3rd-party/prrte//src/mca/prtedl/dlopen/configure.m4:35

# Warning: Excluded: libltdl
# ---------------------------------------- Above from 3rd-party/prrte//src/mca/prtedl/libltdl/configure.m4:46

# Warning: Excluded: libltdl-libdir
# ---------------------------------------- Above from 3rd-party/prrte//src/mca/prtedl/libltdl/configure.m4:49


])
