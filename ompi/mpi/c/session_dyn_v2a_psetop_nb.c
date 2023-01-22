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
//#pragma weak MPI_Session_request_res_change = PMPI_Session_request_res_change
//#endif
//#define MPI_Session_request_res_change PMPI_Session_request_res_change
//#endif

static const char FUNC_NAME[] = "MPI_Session_dyn_v2a_psetop_nb";


int MPI_Session_dyn_v2a_psetop_nb(MPI_Session session, int *op, char **input_sets, int ninput, char *** output_sets, int *noutput, MPI_Info info, MPI_Request *request){
    int rc;

    rc = ompi_instance_dyn_v2a_pset_op_nb((ompi_instance_t *) session, op, input_sets, ninput, output_sets, noutput, (ompi_info_t *) info, (ompi_request_t **) request);

    //ERROR HANDLING      
    OMPI_ERRHANDLER_RETURN (rc, (NULL == session) ? MPI_SESSION_NULL : session, rc, FUNC_NAME);
}
