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

int main(int argc, char* argv[])
{
    int rank, size, len, rc;
    const char pset_name[] = "mpi://SELF";
    char version[MPI_MAX_LIBRARY_VERSION_STRING];
    MPI_Group wgroup = MPI_GROUP_NULL;
    MPI_Session session_handle;
    MPI_Comm lib_comm = MPI_COMM_NULL;

    rc = MPI_Session_init(MPI_INFO_NULL, MPI_ERRORS_RETURN,
                          &session_handle);

    rc = MPI_Group_from_session_pset(session_handle,
                                     pset_name,
                                        &wgroup);
    if (rc != MPI_SUCCESS) {
	printf("MPI_Group_from_Session_pset failed with rc=%d\n", rc);

        MPI_Session_finalize(&session_handle);
        return -1;
    }


   /*
    * get a communicator
    */
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
    //MPI_Comm_size(MPI_COMM_WORLD, &size);


    //MPI_Get_library_version(version, &len);
    printf("Hello, world, I am rank %d\n", rank);
    //MPI_Finalize();

    return 0;
}
