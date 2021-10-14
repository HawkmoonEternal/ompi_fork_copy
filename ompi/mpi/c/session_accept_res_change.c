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

int MPI_Session_accept_res_change(MPI_Session *session, MPI_Info *info, char delta_pset[], char result_pset[], int root, MPI_Comm *comm){
    int rc;
    int my_rank;
    char d_pset[PMIX_MAX_KEYLEN];
    ompi_rc_op_type_t rc_type;

    MPI_Comm_rank(*comm, &my_rank);

    if(my_rank == root){
        ompi_instance_get_rc_type(delta_pset,  &rc_type);
        rc = ompi_instance_accept_res_change(session, (opal_info_t**)info, delta_pset, result_pset);
    }
    MPI_Bcast(&rc, 1, MPI_INT, 0, *comm);
    if(MPI_SUCCESS == rc){
        if(my_rank == root){
            strcpy(d_pset, delta_pset);
        }
        MPI_Bcast(d_pset, PMIX_MAX_KEYLEN, MPI_CHAR, root, *comm);
        MPI_Bcast(&rc_type, 1, MPI_UINT8_T, root, *comm);
        MPI_Bcast(result_pset, MPI_MAX_PSET_NAME_LEN, MPI_CHAR, root, *comm);
        /* If we want to continue running we need to refresh the instance */ 
        
        bool is_root = my_rank == root;

        MPI_Barrier(*comm);

        rc = ompi_mpi_instance_refresh (session, info, d_pset, rc_type, result_pset, is_root);

        if(MPI_SUCCESS != rc){
            if(OPAL_ERR_BAD_PARAM == rc){
                MPI_Comm_free(comm);
                *comm = MPI_COMM_NULL; 
                return MPI_SUCCESS;
            }
            return rc;
        }
        ompi_instance_clear_rc_cache(delta_pset);
    }
    else if(OPAL_ERR_NOT_FOUND == rc)return MPI_ERR_PENDING;
    return rc;
}