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

static const char FUNC_NAME[] = "MPI_Session_dyn_v2a_query_psetop_nb";


int MPI_Session_dyn_v2a_query_psetop_nb(MPI_Session session, char *coll_pset, char * input_pset, int *type, char ***output_psets, int *noutput, MPI_Request *req){
    int rc;

    //PARAM CHECK
    if (NULL == session || MPI_SESSION_NULL == session) {
            return MPI_ERR_ARG;
    }

    rc = ompi_instance_dyn_v2a_query_psetop_nb((ompi_instance_t *) session, coll_pset, input_pset, type, output_psets, noutput, false, (ompi_request_t **) req);
    
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, (NULL == session) ? MPI_SESSION_NULL : session, rc, FUNC_NAME);
}
