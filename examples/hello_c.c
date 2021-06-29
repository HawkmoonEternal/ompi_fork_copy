/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 *
 * Sample MPI "hello world" application in C
 */

#include <stdio.h>
#include "mpi.h"
#include "../ompi/include/ompi_config.h"
#include "../instance/instance.h"
#include "../opal/util/arch.h"
#include "../opal/util/show_help.h"
#include "../opal/util/argv.h"
#include "../opal/runtime/opal_params.h"
#include "../ompi/mca/pml/pml.h"
#include "../ompi/runtime/params.h"
#include "../ompi/interlib/interlib.h"
#include "../ompi/communicator/communicator.h"
#include "../ompi/errhandler/errhandler.h"
#include "../ompi/errhandler/errcode.h"
#include "../ompi/message/message.h"
#include "../ompi/info/info.h"
#include "../ompi/attribute/attribute.h"
#include "../ompi/op/op.h"
#include "../ompi/dpm/dpm.h"
#include "../ompi/file/file.h"
#include "../ompi/mpiext/mpiext.h"
#include "../ompi/mca/hook/base/base.h"
#include "../ompi/mca/op/base/base.h"
#include "../opal/mca/allocator/base/base.h"
#include "../opal/mca/rcache/base/base.h"
#include "../opal/mca/mpool/base/base.h"
#include "../ompi/mca/bml/base/base.h"
#include "../ompi/mca/pml/base/base.h"
#include "../ompi/mca/coll/base/base.h"
#include "../ompi/mca/osc/base/base.h"
#include "../ompi/mca/io/base/base.h"
#include "../ompi/mca/topo/base/base.h"
#include "../opal/mca/pmix/base/base.h"
#include "../opal/mca/mpool/base/mpool_base_tree.h"
#include "../ompi/mca/pml/base/pml_base_bsend.h"
#include "../ompi/util/timings.h"
#include "../opal/mca/pmix/pmix-internal.h"

int MPI_Session_get_res_change(MPI_Session session, MPI_Info *info_used){



    int rc;
    //PARAM CHECK

    
    
    rc=ompi_instance_get_res_change(session, (opal_info_t**)info_used);

    //ERROR HANDLING
    
    //ERROR HANDLING
    //OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);
    return rc;

}

int main(int argc, char* argv[])
{
    printf("start of hello_c\n");
    int rank, size, len, rc;
    const char pset_name[] = "mpi://SELF";
    char version[MPI_MAX_LIBRARY_VERSION_STRING];
    MPI_Group wgroup = MPI_GROUP_NULL;
    MPI_Session session_handle;
    MPI_Comm lib_comm = MPI_COMM_NULL;
    int npset_names;

    rc = MPI_Session_init(MPI_INFO_NULL, MPI_ERRORS_RETURN,
                          &session_handle);
    rc = MPI_Session_get_num_psets (session_handle, MPI_INFO_NULL, &npset_names);

    printf("num psets: %d\n", npset_names);

    rc = MPI_Group_from_session_pset(session_handle,
                                     pset_name,
                                        &wgroup);
    if (rc != MPI_SUCCESS) {
	printf("MPI_Group_from_Session_pset failed with rc=%d\n", rc);

        MPI_Session_finalize(&session_handle);
        return -1;
    }
    int group_size;
    MPI_Group_size(wgroup,&group_size);
    printf("group size: %d\n", group_size);

    MPI_Info info;
    MPI_Session_get_res_change(session_handle, &info);
    
    int flag=0;
    int valuelen=0;
    MPI_Info_get_valuelen(info, "MPI_INFO_KEY_RC_TAG", &valuelen, &flag);
    char* pset[valuelen+1];
    printf("valuelen: %d\n", valuelen);
    MPI_Info_get(info, "MPI_INFO_KEY_RC_TAG", valuelen, pset, &flag);
    pset[valuelen]='\0';
    if(flag)printf("Found a Resource Change with pset %s\n", pset);
   /*
    * get a communicator
    
    rc = MPI_Comm_create_from_group(wgroup, "mpi.forum.example",
                                    MPI_INFO_NULL,
                                    MPI_ERRORS_RETURN,
                                    &lib_comm);
    if (rc != MPI_SUCCESS) {
	 printf("MPI_comm_create_from_group failed\n");

        MPI_Group_free(&wgroup);
        MPI_Session_finalize(&session_handle);
        return -1;
    }
    //MPI_Init(&argc, &argv);
    MPI_Comm_rank(lib_comm, &rank);
    */
    //MPI_Comm_size(MPI_COMM_WORLD, &size);


    //MPI_Get_library_version(version, &len);
    //printf("Hello, world, I am rank %d\n", rank);

    sleep(60);
    MPI_Session_finalize(&session_handle);
    
    return 0;
}
