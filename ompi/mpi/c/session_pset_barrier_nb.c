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
//#pragma weak MPI_Session_pset_create_op = PMPI_Session_pset_create_op
//#endif
//#define MPI_Session_pset_create_op PMPI_Session_pset_create_op
//#endif

static const char FUNC_NAME[] = "MPI_Session_pset_barrier_nb";

int MPI_Session_pset_barrier_nb(MPI_Session session, char **pset_names, int num_psets, MPI_Info info, MPI_Request *req){
    
    int rc;

    if(NULL == pset_names || 1 > num_psets){
        return MPI_ERR_ARG;
    }
    rc = ompi_instance_pset_barrier_nb(pset_names, num_psets, (ompi_info_t *) info, (ompi_request_t **) req);
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, (NULL == session) ? MPI_SESSION_NULL : session,
                            rc, FUNC_NAME);

}