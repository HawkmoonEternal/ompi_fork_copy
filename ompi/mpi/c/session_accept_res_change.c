#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/instance/instance.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"
#include <stdlib.h>
#include <string.h>

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Session_get_res_change = PMPI_Session_get_res_change
#endif
#define MPI_Session_get_res_change PMPI_Session_get_res_change
#endif

static const char FUNC_NAME[] = "MPI_Session_accept_res_change";

int MPI_Session_accept_res_change(MPI_Session *session, MPI_Info *info, char delta_pset[], char result_pset[], int root, MPI_Comm *comm, int *terminate){
    int rc;
    int my_rank;
    int flag;
    bool blocking = false;
    char val[8];
    char d_pset[PMIX_MAX_KEYLEN];
    ompi_rc_op_type_t rc_type;

    *terminate = 0;

    MPI_Comm_rank(*comm, &my_rank);

    if(my_rank == root){
        if(NULL != info && MPI_INFO_NULL != *info){
            MPI_Info_get(*info, "mpi_blocking", 8, val, &flag);
            if(flag && 1 == atoi(val)){
                blocking = true;
            }
        }
        ompi_instance_get_rc_type(delta_pset,  &rc_type);
        rc = ompi_instance_accept_res_change(session, (opal_info_t**)info, delta_pset, result_pset, blocking);
    }
    MPI_Bcast(&rc, 1, MPI_INT, 0, *comm);
    printf("rank: %d, first bcast in accept %d\n", my_rank, rc);
    if(MPI_SUCCESS == rc){
        if(my_rank == root){
            strcpy(d_pset, delta_pset);
        }
        MPI_Bcast(d_pset, PMIX_MAX_KEYLEN, MPI_CHAR, root, *comm);
        printf("rank: %d, second bcast in accept %s\n", my_rank, d_pset);
        MPI_Bcast(&rc_type, 1, MPI_UINT8_T, root, *comm);
        printf("rank: %d, third bcast in accept %d\n", my_rank, rc_type);
        MPI_Bcast(result_pset, MPI_MAX_PSET_NAME_LEN, MPI_CHAR, root, *comm);
        printf("rank: %d, fourth bcast in accept %s\n", my_rank, result_pset);
        /* If we want to continue running we need to refresh the instance */ 
        
        bool is_root = my_rank == root;
        printf("rank: %d, barrier start\n", my_rank);
        //MPI_Barrier(*comm);
        printf("rank: %d, barrier end\n", my_rank);

        //if(rc_type == MPI_RC_SUB){
        //    MPI_Comm_disconnect(comm);
        //}

        printf("rank: %d, refresh\n", my_rank);
        rc = ompi_mpi_instance_refresh (session, info, d_pset, rc_type, result_pset, is_root);
        printf("rank: %d, finished refresh %d\n", my_rank, rc);
        if(MPI_SUCCESS != rc){
            if(OPAL_ERR_BAD_PARAM == rc){
                *terminate = 1;
                return MPI_SUCCESS;
            }
            return rc;
        }
        ompi_instance_clear_rc_cache(delta_pset);
    }
    else if(OPAL_ERR_NOT_FOUND == rc)return MPI_ERR_PENDING;
    return rc;
}