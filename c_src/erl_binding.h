/* 
 * File:   erl_binding.h
 * Author: sidorov
 *
 * Created on January 29, 2014, 6:34 PM
 */

#include <erl_nif.h>
#include <unordered_map>
#include "backend.h"

#ifndef ERL_BINDING_H
#define	ERL_BINDING_H

namespace relliptics
{

class erl_binding {
public:
    static ERL_NIF_TERM open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
    static ERL_NIF_TERM write_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
    static ERL_NIF_TERM read_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
    static ERL_NIF_TERM read_by_indexes_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
    static ERL_NIF_TERM close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
    
    static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);
    static void unload(ErlNifEnv* env, void* priv_data);
    
    static void remove_db_connection(ErlNifEnv* env, void* obj);
private:
    struct db_connection;

    erl_binding(ErlNifEnv* caller_env);
    erl_binding(const erl_binding& orig) = delete;
    ~erl_binding() {}
    
    static erl_binding& instance(ErlNifEnv* env);
    ERL_NIF_TERM make_ok_result(ErlNifEnv* env, ERL_NIF_TERM value);
    db_connection& get_connection(ErlNifEnv* env, ERL_NIF_TERM ref);

    ERL_NIF_TERM open(ErlNifEnv* env, ERL_NIF_TERM config);
    std::string read_string(ErlNifEnv* env, ERL_NIF_TERM value) const;
    std::vector<node_entry> read_nodes(ErlNifEnv* env, ERL_NIF_TERM value) const;
    std::vector<int> read_groups(ErlNifEnv* env, ERL_NIF_TERM value) const;
    
    ERL_NIF_TERM write_async(ErlNifEnv* env, ERL_NIF_TERM caller_ref, ERL_NIF_TERM db_ref, ERL_NIF_TERM data);
    std::vector<std::string> read_stringlist(ErlNifEnv* env, ERL_NIF_TERM value) const;
    
    ERL_NIF_TERM read_async(ErlNifEnv* env, ERL_NIF_TERM caller_ref, ERL_NIF_TERM db_ref, ERL_NIF_TERM keys);
    ERL_NIF_TERM read_by_indexes_async(ErlNifEnv* env, ERL_NIF_TERM caller_ref, ERL_NIF_TERM db_ref, 
                                       ERL_NIF_TERM strategy, ERL_NIF_TERM indexes);
    
    ERL_NIF_TERM close(ErlNifEnv* env, ERL_NIF_TERM db_ref);
    

    
    ErlNifResourceType* _db_resource_type;
    
    static const char* MODULE;           
};

}

#endif	/* ERL_BINDING_H */

