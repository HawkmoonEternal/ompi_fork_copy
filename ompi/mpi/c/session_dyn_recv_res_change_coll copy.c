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


int MPI_Session_dyn_v2_recv_res_change_coll(MPI_Session session, char *coll_pset, char * input_pset, int *type, char ***output_psets, int *noutput, int *incl){
    int rc;
    size_t _noutput = 0;
    char bound_pset[PMIX_MAX_KEYLEN];
    int flag = 0;
    ompi_psetop_type_t ompi_rc_op_type = OMPI_PSETOP_NULL;
    ompi_rc_status_t rc_status = RC_INVALID;
    //PARAM CHECK
    if (NULL == session || MPI_SESSION_NULL == session) {
            return MPI_ERR_ARG;
    }

    rc = ompi_instance_get_res_change_collective((ompi_instance_t *) session, coll_pset, input_pset, &ompi_rc_op_type, output_psets, &_noutput, incl, &rc_status, NULL, false);

    //ERROR HANDLING
    
    *type = MPI_OMPI_CONVT_PSET_OP(ompi_rc_op_type);
    *noutput = (int) _noutput; 

    printf("received: rc_type = %d, rc_status = %d, incl = %d\n", *type, rc_status, *incl);
    
    return rc;
}
