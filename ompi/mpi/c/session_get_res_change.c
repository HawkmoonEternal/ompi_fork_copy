 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/instance/instance.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Session_get_res_change = PMPI_Session_get_res_change
#endif
#define MPI_Session_get_res_change PMPI_Session_get_res_change
#endif

static const char FUNC_NAME[] = "MPI_Session_get_res_change";

int MPI_Session_get_res_change(MPI_Session session, MPI_Info *info_used){



    int rc;
    //PARAM CHECK

    
    
    rc=ompi_instance_get_res_change(session, (opal_info_t**)info_used);

    //ERROR HANDLING
    
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);


}