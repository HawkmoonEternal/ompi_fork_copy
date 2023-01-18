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
//#pragma weak MPI_Session_confitm_res_change = PMPI_Session_comfirm_res_change
//#endif
//#define MPI_Session_confirm_res_change PMPI_Session_confirm_res_change
//#endif

static const char FUNC_NAME[] = "MPI_Session_dyn_request_res_change_nb";


int MPI_Session_dyn_v2c_psetop_nb(MPI_Session session, MPI_Info *setop_infos, int ninfo, int *flag, MPI_Request *req){
    int rc;

    rc = ompi_instance_dyn_v2c_psetop_nb(session, setop_infos, ninfo, *flag, (ompi_request_t **) req);
    return rc;
}