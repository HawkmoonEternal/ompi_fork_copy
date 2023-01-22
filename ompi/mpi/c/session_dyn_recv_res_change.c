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
//#pragma weak MPI_Session_get_res_change = PMPI_Session_get_res_change
//#endif
//#define MPI_Session_get_res_change PMPI_Session_get_res_change
//#endif

static const char FUNC_NAME[] = "MPI_Session_dyn_recv_change";


int MPI_Session_dyn_recv_res_change(MPI_Session session, char * assoc_pset, int *type, char ***delta_psets, int *num_delta_psets, int *incl, MPI_Info *info){
    int rc;

    ompi_psetop_type_t ompi_rc_op_type;
    ompi_rc_status_t rc_status;
    size_t num_delta = 0;
    //PARAM CHECK
    if (NULL == session || MPI_SESSION_NULL == session) {
            return MPI_ERR_ARG;
    }

    rc = ompi_instance_get_res_change(session, assoc_pset, &ompi_rc_op_type, delta_psets, &num_delta, incl, &rc_status, (ompi_info_t **) info, false);
    
    if(rc == OPAL_ERR_NOT_FOUND || rc == OMPI_SUCCESS){
        rc = MPI_SUCCESS;
    }
    //ERROR HANDLING
    
    *type = MPI_OMPI_CONVT_PSET_OP(ompi_rc_op_type);
    *num_delta_psets = (int) num_delta;
    
    OMPI_ERRHANDLER_RETURN (rc, (NULL == session) ? MPI_SESSION_NULL : session, rc, FUNC_NAME);
}
