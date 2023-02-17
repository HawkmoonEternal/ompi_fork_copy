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

static const char FUNC_NAME[] = "MPI_Session_dyn_v2a_query_psetop";


int MPI_Session_dyn_v2a_query_psetop(MPI_Session session, char *coll_pset, char * input_pset, int *type, char ***output_psets, int *noutput){
    int rc;
    size_t _noutput = 0;
    ompi_psetop_type_t ompi_rc_op_type = OMPI_PSETOP_NULL;

    //PARAM CHECK
    if (NULL == session || MPI_SESSION_NULL == session) {
            return MPI_ERR_ARG;
    }

    rc = ompi_instance_dyn_v2a_query_psetop((ompi_instance_t *) session, coll_pset, input_pset, &ompi_rc_op_type, output_psets, &_noutput, false);

    //ERROR HANDLING
    
    *type = MPI_OMPI_CONVT_PSET_OP(ompi_rc_op_type);
    *noutput = (int) _noutput; 
    
    OMPI_ERRHANDLER_RETURN (rc, (NULL == session) ? MPI_SESSION_NULL : session, rc, FUNC_NAME);
}