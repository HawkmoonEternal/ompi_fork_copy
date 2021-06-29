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
#pragma weak MPI_Session_pset_op = PMPI_Session_pset_op
#endif
#define MPI_Session_pset_op PMPI_Session_pset_op
#endif

#define MPI_Session_pset_op_ADD 0
#define MPI_Session_pset_op_SUB 1
#define MPI_Session_pset_op_CUT 2
static const char FUNC_NAME[] = "MPI_Session_pset_op";

int MPI_Session_pset_op(MPI_session session, MPI_Info hints, int op, const char *pset1, const char *pset2, const char *pset_result){

    int rc;
    //PARAM CHECK

    
    /*
    if ( op == MPI_Session_pset_op_ADD) 
        rc= ompi_instance_pset_add (session, pset1, pset2, pset_result, (opal_info_t **) hints);
    else if ( op == MPI_Session_pset_op_SUB)
        rc= ompi_instance_pset_add (session, pset1, pset2, pset_result, (opal_info_t **) hints);
    else 
        rc= ompi_instance_pset_add (session, pset1, pset2, pset_result, (opal_info_t **) hints);
    */
    //ERROR HANDLING
    
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);


}