#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/instance/instance.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"
#include <stdlib.h>
#include <string.h>

//#if OMPI_BUILD_MPI_PROFILING
//#if OPAL_HAVE_WEAK_SYMBOLS
//#pragma weak MPI_Session_accept_res_change = PMPI_Session_accept_res_change
//#endif
//#define MPI_Session_get_accept_change PMPI_Session_accept_res_change
//#endif

static const char FUNC_NAME[] = "MPI_Session_accept_res_change";

int MPI_Session_accept_res_change(MPI_Session *session, MPI_Info *info, char delta_pset[], char result_pset[], int root, MPI_Comm *comm, int *terminate){
    int rc;
    int my_rank = -1;
    int flag;
    bool blocking = false;
    bool disconnect = true;
    char val[8];
    char val2[8];
    char val_rc_type[8];
    char d_pset[PMIX_MAX_KEYLEN];
    ompi_rc_op_type_t rc_type = OMPI_RC_NULL;

    *terminate = 0;

    /* check if user whishes to disconnect from comm */
    if(NULL != info && MPI_INFO_NULL != *info){
        MPI_Info_get(*info, "mpi_no_disconnect", 8, val2, &flag);
        if(flag && 1 == atoi(val2)){
            disconnect = false;
        }
    }

    /* no communicator given so lookup rc_op_type in info object */
    if(NULL == comm){
    	MPI_Info_get(*info, "mpi_rc_type", 8, val_rc_type, &flag);
        if(flag){
	    rc_type = (ompi_rc_op_type_t)atoi(val_rc_type);
        }
    }

    /* root of communicator executes the publish/lookup */
    if(NULL != comm){
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
    }

    /* the publish/lookup suceeded */
    if(MPI_SUCCESS == rc){

        
        if(NULL == comm || my_rank == root){
            strcpy(d_pset, delta_pset);
        }

	/* root broadcasts relevant info via the provided communicator */
        if(NULL != comm){
            MPI_Bcast(d_pset, PMIX_MAX_KEYLEN, MPI_CHAR, root, *comm);
            MPI_Bcast(&rc_type, 1, MPI_UINT8_T, root, *comm);
            MPI_Bcast(result_pset, MPI_MAX_PSET_NAME_LEN, MPI_CHAR, root, *comm);
            /* If we want to continue running we need to refresh the instance */ 
    

            if(disconnect){
                MPI_Barrier(*comm);
                if(rc_type == MPI_RC_SUB){
                    MPI_Comm_disconnect(comm);
                }
            }
        }

        bool is_root = my_rank == root;
        rc = ompi_mpi_instance_refresh (session, info, d_pset, rc_type, result_pset, is_root);
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
