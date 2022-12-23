/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018-2022 Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "instance.h"

#include "opal/util/arch.h"

#include "opal/util/show_help.h"
#include "opal/util/argv.h"
#include "opal/runtime/opal_params.h"

#include "ompi/mca/pml/pml.h"
#include "ompi/runtime/params.h"

#include "ompi/interlib/interlib.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/errhandler/errcode.h"
#include "ompi/message/message.h"
#include "ompi/info/info.h"
#include "ompi/attribute/attribute.h"
#include "ompi/op/op.h"
#include "ompi/dpm/dpm.h"
#include "ompi/file/file.h"
#include "ompi/mpiext/mpiext.h"

#include "ompi/mca/hook/base/base.h"
#include "ompi/mca/op/base/base.h"
#include "opal/mca/allocator/base/base.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/mca/smsc/base/base.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/part/base/base.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/mca/topo/base/base.h"
#include "opal/mca/pmix/base/base.h"

#include "opal/mca/mpool/base/mpool_base_tree.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/util/timings.h"
#include "opal/mca/pmix/pmix-internal.h"

#include "ompi/instance/instance_psets.h"


int ompi_instance_psets_ref_count = 0;
int mutex_counter = 0;
/* List of local Psets */
static opal_list_t ompi_mpi_instance_psets;

/* Lock the lists of resource changes and PSets */
static opal_recursive_mutex_t res_changes_and_psets_lock;

size_t num_pmix_psets = 0;
size_t num_builtin_psets = 0;

int ompi_instance_psets_init(){

    if(ompi_instance_psets_ref_count == 0){
        OBJ_CONSTRUCT(&res_changes_and_psets_lock, opal_recursive_mutex_t);
        OBJ_CONSTRUCT(&ompi_mpi_instance_psets, opal_list_t);
    }

    ++ompi_instance_psets_ref_count;

    return OMPI_SUCCESS;
}

int ompi_instance_psets_finalize(){

    --ompi_instance_psets_ref_count;

    if(0 == ompi_instance_psets_ref_count){
        OBJ_DESTRUCT(&res_changes_and_psets_lock);
        OBJ_DESTRUCT(&ompi_mpi_instance_psets);
    }

    return OMPI_SUCCESS;
}

bool ompi_instance_psets_initalized(){
    return ompi_instance_psets_ref_count > 0;
}

int ompi_instance_builtin_psets_init(int n_builtin_psets, char **names, opal_process_name_t **members, size_t *nmembers, char **aliases){
    int n, i;
    ompi_mpi_instance_pset_t *pset_ptr, *alias_pset_ptr;

    num_builtin_psets = n_builtin_psets;
    
    if(!ompi_instance_psets_initalized()){
        ompi_instance_psets_init();
    }

    ompi_instance_lock_rc_and_psets();

    for(n = 0; n < n_builtin_psets; n++){
        pset_ptr = OBJ_NEW(ompi_mpi_instance_pset_t);
        strcpy(pset_ptr->name, names[n]);

        if(NULL != members){
            if(NULL != nmembers && 0 != nmembers[n]){
                pset_ptr->size = nmembers[n];
                pset_ptr->members = (opal_process_name_t *) malloc(nmembers[n] * sizeof(opal_process_name_t));
                memcpy(pset_ptr->members, members[n], nmembers[n] * sizeof(opal_process_name_t));
            }
        }
        if(NULL != aliases){
            if(NULL != aliases[n]){
                /* create the alias PSet */
                alias_pset_ptr = OBJ_NEW(ompi_mpi_instance_pset_t);
                strcpy(alias_pset_ptr->name, aliases[n]);
                add_pset(alias_pset_ptr);

                /* Add the alias PSet label to the builtin PSet */
                pset_ptr->alias = strdup(aliases[n]);
            }
        }
        add_pset(pset_ptr);
    }

    ompi_instance_unlock_rc_and_psets();

    return OMPI_SUCCESS;

}

void ompi_instance_lock_rc_and_psets(){

    opal_mutex_lock(&res_changes_and_psets_lock);

}

void ompi_instance_unlock_rc_and_psets(){

    opal_mutex_unlock(&res_changes_and_psets_lock);

}

#pragma region PSets

static void pset_destructor(ompi_mpi_instance_pset_t *pset){
    free(pset->members);
    free(pset->alias);
}

static void pset_constructor(ompi_mpi_instance_pset_t *pset){
    pset->size = 0;
    pset->members = NULL;
    pset->alias = NULL;
    pset->flags = OMPI_PSET_FLAG_NONE;
}

OBJ_CLASS_INSTANCE(ompi_mpi_instance_pset_t, opal_object_t, pset_constructor, pset_destructor);

/* cache/update pset locally */
void pset_define_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){

    size_t n, sz, nmembers=0;

    pmix_status_t rc=PMIX_SUCCESS;
    pmix_proc_t *data_array;
    pmix_proc_t *members;

    char pset_name[PMIX_MAX_KEYLEN]={0};

    //ompi_instance_lock_rc_and_psets();
    //for(n=0; n<ninfo; n++){
    //    if(0 == strcmp(info[n].key, PMIX_PSET_NAME)){
    //        strncpy(pset_name, info[n].value.data.string, PMIX_MAX_KEYLEN);
    //    }else if(0 == strcmp(info[n].key, PMIX_PSET_MEMBERS)){
    //            data_array=(pmix_proc_t*)info[n].value.data.darray->array;
    //            nmembers=info[n].value.data.darray->size;
    //    }
    //}
    //bool new_pset=false;
    //if(strlen(pset_name) > 0){
    //    ompi_mpi_instance_pset_t *pset;
    //    if(NULL == (pset = get_pset_by_name(pset_name))){
    //        pset=OBJ_NEW(ompi_mpi_instance_pset_t);
    //        new_pset=true;
    //    }
    //    strncpy(pset->name, pset_name, PMIX_MAX_KEYLEN);
    //    pset->malleable=true;
    //    pset->active=true;
    //    pset->size=nmembers;
//
    //    if(0 < nmembers){
    //        pset->size = nmembers;
    //        pset->members = malloc(nmembers*sizeof(opal_process_name_t));
    //        for(n=0; n < nmembers; n++){
    //            OPAL_PMIX_CONVERT_PROCT(rc, &pset->members[n], &data_array[n]);
    //        }
    //    }
    //    if(new_pset){
    //        opal_list_append(&ompi_mpi_instance_psets, &pset->super);
    //    }
    //}
//
    //ompi_instance_unlock_rc_and_psets();

    cbfunc(PMIX_EVENT_ACTION_COMPLETE, NULL, 0, NULL, NULL, cbdata);
}

/* set local pset to inactive */
void pset_delete_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){

    size_t n, sz;
    pmix_status_t rc = PMIX_SUCCESS;
    char *pset_name = NULL;
    

    ompi_instance_lock_rc_and_psets();
    for(n=0; n<ninfo; n++){
        if(0 == strcmp(info[n].key, PMIX_PSET_NAME)){
            PMIX_VALUE_UNLOAD(rc, &info[n].value, (void**)&pset_name, &sz);
        }
    }
    if(NULL != pset_name){
        ompi_mpi_instance_pset_t *pset;
        if(NULL != (pset=get_pset_by_name(pset_name))){
            pset->active=false;
        }
        free(pset_name);
    }

    ompi_instance_unlock_rc_and_psets();

    cbfunc(PMIX_SUCCESS, NULL, 0, NULL, NULL, cbdata);
}

#pragma region utils

size_t get_num_builtin_psets(){ 
    return num_builtin_psets;
}

size_t get_num_pmix_psets(){ 
    return num_pmix_psets;
}

size_t get_nth_pset_name_length(int n){
    ompi_mpi_instance_pset_t * pset;
    
    pset = get_nth_pset(n);
    
    return strlen(pset->name);
}

char * get_nth_pset_name(int n, char *pset_name, size_t len){
    return strncpy (pset_name, (get_nth_pset(n))->name, len);
}

int add_pset(ompi_mpi_instance_pset_t *pset){
    ompi_instance_lock_rc_and_psets();

    if(NULL != get_pset_by_name(pset->name)){
        ompi_instance_unlock_rc_and_psets();
        return OMPI_ERR_BAD_PARAM;
    }

    opal_list_append(&ompi_mpi_instance_psets, &pset->super);

    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}

int ompi_instance_get_launch_pset(char **pset_name, pmix_proc_t *proc){
    pmix_info_t *results, *info;
    size_t n_results, ninfo, k, n;
    pmix_query_t query;
    pmix_status_t rc;

    PMIX_QUERY_CONSTRUCT(&query);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_LAUNCH_PSET);

    PMIX_INFO_CREATE(query.qualifiers, 1);
    PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_PROCID, proc, PMIX_PROC);
    query.nqual = 1;

    rc = PMIx_Query_info(&query, 1, &results, &n_results);
    if(PMIX_SUCCESS != rc){
        return rc;
    }

    for(n = 0; n < n_results; n++){
        if(PMIX_CHECK_KEY(&results[n], PMIX_QUERY_RESULTS)){
            info = results[n].value.data.darray->array;
            ninfo = results[n].value.data.darray->size;

            for(k = 0; k < ninfo; k++){
                if(PMIX_CHECK_KEY(&info[k], PMIX_QUERY_LAUNCH_PSET)){
                    *pset_name = strdup(info[k].value.data.string);
                    return PMIX_SUCCESS;
                }
            }

        }
    }
    return PMIX_ERR_NOT_FOUND;
}



static void refresh_psets_complete (pmix_status_t status, 
		                                  pmix_info_t *results,
		                                  size_t nresults,
                                                  void *cbdata, 
                                                  pmix_release_cbfunc_t release_fn,
                                                  void *release_cbdata)
{
    size_t n, i, k, ninfo;
    pmix_status_t rc;
    size_t sz;
    size_t n_pmix_psets = 0;
    char *pset_names = NULL;
    char **pset_names_ptr = &pset_names;
    pmix_info_t * info;

    opal_pmix_lock_t *lock = (opal_pmix_lock_t *) cbdata;
    for(k = 0; k < nresults; k++){
        
        if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){

            info = results[k].value.data.darray->array;
            ninfo = results[k].value.data.darray->size;

            for (n = 0; n < ninfo; n++) {
                if (0 == strcmp(info[n].key, PMIX_QUERY_NUM_PSETS)) {
                    PMIX_VALUE_UNLOAD(rc,
                                      &info[n].value,
                                      (void **)&n_pmix_psets,
                                      &sz);

                    num_pmix_psets = n_pmix_psets;
                } else if (0 == strcmp (info[n].key, PMIX_QUERY_PSET_NAMES)) {

                    char** names = opal_argv_split (info[n].value.data.string, ',');
                    size_t num_names = opal_argv_count(names);
                    ompi_instance_lock_rc_and_psets();
                    /* add psets we didn't know about before the query*/
                    for(i = 0; i < num_names; i++){
                        if(NULL == get_pset_by_name(names[i])){
                            ompi_mpi_instance_pset_t *new_pset;
                            new_pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                            strcpy(new_pset->name, names[i]);
                            new_pset->size = 0;
                            new_pset->members = NULL;
                            new_pset->malleable = false;
                            opal_list_append(&ompi_mpi_instance_psets, &new_pset->super);
                        }
                    }
                    num_pmix_psets = opal_list_get_size(&ompi_mpi_instance_psets) - num_builtin_psets;
                    free(pset_names);
                    ompi_instance_unlock_rc_and_psets();
                }
            }
        }
    }

    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    OPAL_PMIX_WAKEUP_THREAD(lock);
}

int refresh_pmix_psets (const char *key)
{
    pmix_status_t rc;
    pmix_query_t query;
    opal_pmix_lock_t lock;
    bool refresh = true;



    PMIX_QUERY_CONSTRUCT(&query);
    PMIX_ARGV_APPEND(rc, query.keys, key);
    PMIX_INFO_CREATE(query.qualifiers, 1);
    query.nqual = 1;
    PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);

    OPAL_PMIX_CONSTRUCT_LOCK(&lock);

    /*
     * TODO: need to handle this better
     */
    rc = PMIx_Query_info_nb(&query, 1, refresh_psets_complete, (void*)&lock);

    OPAL_PMIX_WAIT_THREAD(&lock);
    OPAL_PMIX_DESTRUCT_LOCK(&lock);

    return rc;
}

int pset_init_flags(char *pset_name){
    pmix_query_t query;
    ompi_mpi_instance_pset_t *pset_ptr;
    ompi_psetop_type_t op_type;
    pmix_info_t *results, *result_infos;
    pmix_proc_t * pset_members;
    int rc;
    bool refresh = true;

    size_t nresults, nresult_infos, ninfo, k, i, j;
    if(NULL == (pset_ptr = get_pset_by_name(pset_name))){
        refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    }
    if(NULL == (pset_ptr = get_pset_by_name(pset_name))){
        return OMPI_ERR_NOT_FOUND;
    }

    PMIX_QUERY_CONSTRUCT(&query);
    PMIX_INFO_CREATE(query.qualifiers, 2);
    query.nqual = 2;
    PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
    PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PSET_NAME, pset_ptr->name, PMIX_STRING);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSET_MEMBERSHIP);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_PSET_SOURCE_OP);

    if(PMIX_SUCCESS != (rc = PMIx_Query_info(&query, 1, &results, &nresults))){
        return rc;
    }
    
    for(k = 0; k < nresults; k++){
        if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){
            
            result_infos = (pmix_info_t *) results[k].value.data.darray->array;
            nresult_infos = results[k].value.data.darray->size;
            if(nresult_infos >= 3){
                ompi_instance_lock_rc_and_psets();
                OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_INIT);
                for (i = 0; i < nresult_infos; i++) {
                    if (0 == strcmp (result_infos[i].key, PMIX_QUERY_PSET_MEMBERSHIP)) {
                        pset_ptr->size = result_infos[i].value.data.darray->size;
                        pset_members = (pmix_proc_t *) result_infos[i].value.data.darray->array;
                        for(j = 0; j < pset_ptr->size; j++){
                            if(PMIX_CHECK_PROCID(&pset_members[j], &opal_process_info.myprocid)){
                                OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_INCLUDED);
                                if(j == 0){
                                    OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_PRIMARY);
                                }
                            }
                        }
                    } else if (0 == strcmp(result_infos[i].key, PMIX_PSET_SOURCE_OP)) {
                        op_type = result_infos[i].value.data.uint8;
                        if(MPI_RC_ADD == op_type){
                            OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_DYN);
                        }
                    }
                }
                ompi_instance_unlock_rc_and_psets();
            }
        }
    }

    return OMPI_SUCCESS;
}

ompi_mpi_instance_pset_t * get_pset_by_name(char *name){
    ompi_instance_lock_rc_and_psets();

    ompi_mpi_instance_pset_t *pset_out = NULL;
    OPAL_LIST_FOREACH(pset_out, &ompi_mpi_instance_psets, ompi_mpi_instance_pset_t){
        if(0 == strcmp(name, pset_out->name)){
            if(NULL != pset_out->alias){
                pset_out = get_pset_by_name(pset_out->alias);
            }
            ompi_instance_unlock_rc_and_psets();
            return pset_out;
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL;
}

ompi_mpi_instance_pset_t * get_nth_pset( int n){
    ompi_instance_lock_rc_and_psets();
    int count=0;
    ompi_mpi_instance_pset_t *pset_out=NULL;
    OPAL_LIST_FOREACH(pset_out, &ompi_mpi_instance_psets, ompi_mpi_instance_pset_t){
        if(count++==n){
            ompi_instance_unlock_rc_and_psets();
            return pset_out;
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL;
}

bool is_pset_leader(pmix_proc_t *pset_members, size_t nmembers, pmix_proc_t proc){
    size_t n;
    for(n = 0; n < nmembers; n++){
        int nspace_cmp = strcmp(proc.nspace, pset_members[n].nspace);
        if( 0 < nspace_cmp || (0 == nspace_cmp && pset_members[n].rank < proc.rank))return false;
    }
    return true;
}

/* Local only! */
int is_pset_element(char * pset_name, int *flag){
    opal_process_name_t *procs = NULL;
    ompi_mpi_instance_pset_t *pset;
    size_t nprocs;

    if(NULL == (pset = get_pset_by_name(pset_name)) || NULL == pset->members){
        return OMPI_ERR_NOT_FOUND;
    }

    get_pset_membership(pset->name, &procs, &nprocs);
    *flag = (opal_is_pset_member(procs, nprocs, opal_process_info.my_name) ? 1 : 0);

    return OMPI_SUCCESS;
}

bool is_pset_member(pmix_proc_t *pset_members, size_t nmembers, pmix_proc_t proc){

    size_t n;
    for(n = 0; n < nmembers; n++){
        if(0 == strcmp(proc.nspace,pset_members[n].nspace) && pset_members[n].rank==proc.rank)return true;
    }
    return false;
}

bool opal_is_pset_member( opal_process_name_t *procs, size_t nprocs, opal_process_name_t proc){
    
    size_t n;
    for(n = 0; n < nprocs; n++){

        if(proc.jobid == procs[n].jobid &&  proc.vpid == procs[n].vpid){
            return true;
        }
    }
    return false;
}

bool opal_is_pset_member_local( char *pset_name, opal_process_name_t proc){
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_pset_t *pset = get_pset_by_name(pset_name);
    if(NULL == pset)return false;

    size_t n;
    for(n = 0; n < pset->size; n++){
        if(proc.jobid == pset->members[n].jobid && pset->members[n].vpid == proc.vpid){
            ompi_instance_unlock_rc_and_psets();
            return true;
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return false;
}

int get_pset_size(char *pset_name, size_t *pset_size){
    ompi_mpi_instance_pset_t *pset;
    opal_process_name_t *procs;
    size_t size;
    int rc;

    if(NULL == pset_name){
        return OMPI_ERR_BAD_PARAM;
    }

    pset = get_pset_by_name(pset_name);

    if(NULL == pset|| 0 == pset->size){
        rc = get_pset_membership(pset_name, &procs, &size);
        ompi_instance_free_pset_membership(pset_name);
        if(rc != OMPI_SUCCESS){
            return rc;
        }
    }
    *pset_size = size;

    return OMPI_SUCCESS;
}

/* pset utility functions. Might need to be shifted elsewhere */
int opal_pmix_proc_array_conv(opal_process_name_t *opal_procs, pmix_proc_t **pmix_procs, size_t nprocs){
    pmix_proc_t *pmix_proc_array= *pmix_procs=malloc(nprocs*sizeof(pmix_proc_t));
    int n;
    for(n=0; n<nprocs; n++){
        OPAL_PMIX_CONVERT_NAME(&pmix_proc_array[n], &opal_procs[n]);
    }
    return OMPI_SUCCESS;
}

int pmix_opal_proc_array_conv(pmix_proc_t *pmix_procs, opal_process_name_t **opal_procs, size_t nprocs){
    opal_process_name_t *opal_proc_array= *opal_procs=malloc(nprocs*sizeof(pmix_proc_t));
    int n;
    int rc;
    for(n=0; n<nprocs; n++){
        OPAL_PMIX_CONVERT_PROCT(rc, &opal_proc_array[n], &pmix_procs[n]);
    }
    return OMPI_SUCCESS;
}
#pragma endregion

#pragma region membership
void get_pset_membership_complete(pmix_status_t status, pmix_info_t *results, size_t nresults, 
                void *cbdata, 
                pmix_release_cbfunc_t release_fn, void *release_cbdata)
{
    size_t k, i, n, ninfo, nqualifiers;
    int rc;
    char *pset_name;
    bool new_pset;
    pmix_info_t *info, *qualifiers;
    ompi_mpi_instance_pset_t *pset;

    opal_pmix_lock_t *lock = (opal_pmix_lock_t *) cbdata;

    for(k = 0; k < nresults; k++){

        if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){

            info = (pmix_info_t *) results[k].value.data.darray->array;
            ninfo = results[k].value.data.darray->size;

            /* Find the Pset name qualifiers of these members */
            pset_name = NULL;
            for(n = 0; n < ninfo; n++){
                if(0 == strcmp(info[n].key, PMIX_QUERY_QUALIFIERS)){

                    qualifiers = (pmix_info_t *) info[n].value.data.darray->array;
                    nqualifiers = info[n].value.data.darray->size;

                    for(i = 0; i < nqualifiers; i++){
                        if(0 == strcmp(qualifiers[i].key, PMIX_PSET_NAME)){
                            pset_name = qualifiers[n].value.data.string;
                            break;
                        }
                    }
                    if(NULL != pset_name){
                        break;
                    }
                }
            }

            /* No Pset Qualifier found for this results. Skip. */
            if(NULL == pset_name){
                continue;
            }


            new_pset = (NULL == (pset = get_pset_by_name(pset_name)));

            /* Insert the members in the list of PSets */
            for(n = 0; n < ninfo; n++){
                if(0 == strcmp(info[n].key, PMIX_QUERY_PSET_MEMBERSHIP)){

                    /* Don't have this PSset yet. Insert a new one in the list. */
                    if(new_pset){
                        pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                        strcpy(pset->name, pset_name);
                        opal_list_append(&ompi_mpi_instance_psets, &pset->super);
                    }

                    /* members are not yet set. Set it to the query results */
                    if(NULL == pset->members){


                        pmix_data_array_t *data_array = info[n].value.data.darray;
                        pmix_proc_t *members_array = (pmix_proc_t*) data_array->array;

                        pset->size = data_array->size;
                        pset->members = calloc(pset->size, sizeof(opal_process_name_t));

                        for(i = 0; i < pset->size; i++){
                            OPAL_PMIX_CONVERT_PROCT(rc, &pset->members[i], &members_array[i]);
                        }
                    }
                }
            }
        }
    }

    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    if(NULL != lock){
        OPAL_PMIX_WAKEUP_THREAD(lock);
    }

}
/* get the members of the specified PSet 
 * This function will allocate a members array in the list of PSet structs 
 * The 'members' array should be freed using ompi_instance_free_pset_membership
 */
int get_pset_membership (char *pset_name, opal_process_name_t **members, size_t *nmembers){
    
    pmix_status_t rc;
    int ret;
    opal_pmix_lock_t lock;
    bool refresh = true;
    pmix_info_t *info, *results;
    size_t i, n, k, ninfo, nresults;
    pmix_query_t query;
    char *key = PMIX_QUERY_PSET_MEMBERSHIP;

    ompi_instance_lock_rc_and_psets();

    ompi_mpi_instance_pset_t *pset = get_pset_by_name(pset_name);
    bool new_pset = (pset == NULL);

    /* query the runtime if we do not yet have the PSet membership stored in the list of PSet structs */
    if(NULL == pset || NULL == pset->members || 0 == pset->size){ 
        ompi_instance_unlock_rc_and_psets();

        /* set query keys */
        PMIX_QUERY_CONSTRUCT(&query);
        PMIX_ARGV_APPEND(rc, query.keys, key);

        query.nqual = 2;
        PMIX_INFO_CREATE(query.qualifiers, 2);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PSET_NAME, pset_name, PMIX_STRING);

        /* Send the query */
        if (PMIX_SUCCESS != (rc = PMIx_Query_info(&query, 1, &results, &nresults)) || 0 == nresults) {
            ret = opal_pmix_convert_status(rc);
            return ret;                                         
        }

        /* set pset members in the list of local PSets */
        ompi_instance_lock_rc_and_psets();

        for(k = 0; k < nresults; k++){

            if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){
                
                info = results[k].value.data.darray->array;
                ninfo = results[k].value.data.darray->size;

                for(n = 0; n < ninfo; n++){
                    if(0 == strcmp(info[n].key, key)){
                        if(new_pset){
                            pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                            strcpy(pset->name, pset_name);
                        }

                        pmix_data_array_t *data_array = info[n].value.data.darray;
                        pmix_proc_t *members_array = (pmix_proc_t*) data_array->array;

                        pset->size = data_array->size;
                        *nmembers = pset->size;
                        pset->members = calloc(*nmembers, sizeof(opal_process_name_t));
                        for(i = 0; i < *nmembers; i++){
                            OPAL_PMIX_CONVERT_PROCT(rc, &pset->members[i], &members_array[i]);
                        }

                        if(new_pset){
                            opal_list_append(&ompi_mpi_instance_psets, &pset->super);
                        }

                        *members = pset->members;

                    }
                }
            }
        }
        PMIX_INFO_FREE(results, nresults);   

    /* If we already have this membership, do a lookup in the local list of PSet structs */    
    }else{
        if(NULL == pset->members){
            ompi_instance_unlock_rc_and_psets();
            return PMIX_ERR_NOT_FOUND;
        }
        *nmembers = pset->size;
        *members = pset->members;
    }
    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}

/* Free the allocated mebership in the list of PSet structs 
 * The membership was allocated by get_pset_membership
 */
int ompi_instance_free_pset_membership ( char *pset_name){
    ompi_mpi_instance_pset_t *pset = NULL;

    ompi_instance_lock_rc_and_psets();
    pset = get_pset_by_name(pset_name);

    if(NULL != pset && NULL != pset->members){
        free(pset->members);
        pset->members = NULL;
    }
    ompi_instance_unlock_rc_and_psets();

    return OMPI_SUCCESS;
}
#pragma endregion

#pragma region fence
static void fence_release(pmix_status_t status, void *cbdata)
{
    volatile bool *active = (volatile bool*)cbdata;
    OPAL_ACQUIRE_OBJECT(active);
    *active = false;
    OPAL_POST_OBJECT(active);
}


int ompi_instance_pset_fence(char *pset_name){

    pmix_status_t rc;
    int ret, i;
    volatile bool active = true;
    bool flag = true;
    pmix_info_t info;
    opal_process_name_t *opal_proc_names;
    pmix_proc_t *procs;
    size_t nprocs;

    /* retrieve pset members TODO: procs are opal_processes */  
    get_pset_membership(pset_name, &opal_proc_names, &nprocs);

    procs = malloc(nprocs * sizeof(pmix_proc_t));
    for(i = 0; i < nprocs; i++){
            OPAL_PMIX_CONVERT_NAME(&procs[i], &opal_proc_names[i]);
    }
    
    /* Perform the fence operation across all pset members */
    OPAL_POST_OBJECT(&active);
    PMIX_INFO_LOAD(&info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
    if (PMIX_SUCCESS != (rc = PMIx_Fence_nb(NULL, 0, &info, 1,
                                            fence_release, (void*)&active))) {
        ret = opal_pmix_convert_status(rc);
        return ret;
    }
    OMPI_LAZY_WAIT_FOR_COMPLETION(active);
    
    ompi_instance_free_pset_membership(pset_name);
    free(procs);
    return OMPI_SUCCESS;
}

/* Executes a fence operation over the union of the specified PSets */
int ompi_instance_pset_fence_multiple(char **pset_names, int num_psets, ompi_info_t *info){

    pmix_status_t rc;
    int ret;
    volatile bool active = true;
    bool flag = false;
    bool found = false;
    pmix_proc_t **procs;
    pmix_proc_t *fence_procs;
    pmix_info_t fence_info;
    size_t *nprocs;
    size_t max_procs = 0;
    size_t num_fence_procs = 0;

    ompi_mpi_instance_pset_t *pset_ptr;
    opal_process_name_t * opal_proc_names;

    refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    
    /* allocate array of pset sizes */
    nprocs = malloc(num_psets * sizeof(size_t));

    /* allocate array of proc arrays */
    procs = malloc(num_psets * sizeof(pmix_proc_t *));

    for(int i = 0; i < num_psets; i++){
        /* retrieve pset members */
        if(NULL == (pset_ptr = get_pset_by_name(pset_names[i]))){
            free(nprocs);
            free(procs);
            return OMPI_ERR_NOT_FOUND;
        }
        get_pset_membership(pset_ptr->name, &opal_proc_names, &nprocs[i]);

        procs[i] = malloc(nprocs[i] * sizeof(pmix_proc_t));
        for(int j = 0; j < nprocs[i]; j++){
            OPAL_PMIX_CONVERT_NAME(&procs[i][j], &opal_proc_names[j]);
        }
        max_procs += nprocs[i];
    }

    /* allocate an array of pmix_proc_t assuming non-overlapping PSets. We shrink it afterwards */
    fence_procs = malloc(max_procs * sizeof(pmix_proc_t));

    /* Iterate over all PSets and insert their members in the fence_procs array if they are not yet inserted */
    for(int i = 0; i < num_psets; i++){
        for(int j = 0; j < nprocs[i]; j++){
            found = false;
            pmix_proc_t proc_to_insert = procs[i][j];
            for(int k = 0; k < num_fence_procs; k++){
                if(PMIX_CHECK_PROCID(&proc_to_insert, &fence_procs[k])){
                    found = true;
                    break;
                }
            }
            if(!found){
                fence_procs[num_fence_procs++] = proc_to_insert;
            }
        }
    }

    /* now resize the array of procs accordingly */
    fence_procs = realloc(fence_procs, num_fence_procs * sizeof(pmix_proc_t));
    
    
    /* Perform the fence operation across the UNION of the pset members */
    PMIX_INFO_CONSTRUCT(&fence_info);
    PMIX_INFO_LOAD(&fence_info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);

    OPAL_POST_OBJECT(&active);
    if (PMIX_SUCCESS != (rc = PMIx_Fence_nb(fence_procs, num_fence_procs, &fence_info, 1,
                                            fence_release, (void*)&active))) {
        ret = opal_pmix_convert_status(rc);
        return ret;
    }
    OMPI_LAZY_WAIT_FOR_COMPLETION(active);
    

    /* Clean up */
    PMIX_INFO_DESTRUCT(&fence_info);

    for(int i = 0; i < num_psets; i++){
        pset_ptr = get_pset_by_name(pset_names[i]);
        ompi_instance_free_pset_membership(pset_ptr->name);
        free(procs[i]);
    }

    free(fence_procs);
    free(nprocs);
    free(procs);
    
    return OMPI_SUCCESS;
}

#pragma endregion
