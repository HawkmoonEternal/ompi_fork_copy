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

static const char FUNC_NAME[] = "MPI_Session_pset_op";

int MPI_Session_pset_op(MPI_Session session, int op, char **input_sets, int ninput, char *** output_sets, int *noutput, MPI_Info info){

    int rc, flag = 0;
    char pref_name[PMIX_MAX_KEYLEN];
    ompi_rc_op_type_t ompi_op = MPI_OMPI_CONV_PSET_OP(op);
    //PARAM CHECK
    if(NULL == input_sets || 0 == ninput || ompi_op == OMPI_PSETOP_NULL){
        return MPI_ERR_ARG;
    }

    rc = ompi_instance_pset_op(session, ompi_op, input_sets, ninput, output_sets, noutput, info);
    
    //ERROR HANDLING
    
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);
    return rc;

}