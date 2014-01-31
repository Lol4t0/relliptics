/* 
 * File:   erl_binding.cpp
 * Author: sidorov
 * 
 * Created on January 29, 2014, 6:34 PM
 */

#include "erl_binding.h"
#include "backend.h"
#include <erl_nif.h>
#include <assert.h>
#include <elliptics/error.hpp>

namespace relliptics
{
    
namespace atoms
{
    ERL_NIF_TERM LOG_PATH;
    ERL_NIF_TERM NODES;
    ERL_NIF_TERM GROUPS;
    ERL_NIF_TERM NAMESPACE;
    
    ERL_NIF_TERM OK;
    ERL_NIF_TERM ERROR;
    ERL_NIF_TERM BACKEND_ERROR;
    
    ERL_NIF_TERM DATA;
    
    ERL_NIF_TERM ALL;
    ERL_NIF_TERM ANY;
}

class callback
{
public:
    callback(ErlNifEnv* original_env, ERL_NIF_TERM ref):
        _self_env(enif_alloc_env(), &enif_free_env),
        _ref(enif_make_copy(&*_self_env, ref))
    {
        enif_self(original_env, &_pid);
    }
        
        
private:
    std::shared_ptr<ErlNifEnv> _self_env;
    ERL_NIF_TERM _ref;
    ErlNifPid _pid;
    
protected:
    void send_reply(ERL_NIF_TERM term)
    {
        ERL_NIF_TERM refd_term = enif_make_tuple2(env(), enif_make_copy(env(), _ref), term);
        enif_send(0, &_pid, env(), refd_term);
    }
    
    ErlNifEnv* env()
    {
        return &*_self_env;
    }
};
    
class complete_callback: public callback
{
public:
    complete_callback(ErlNifEnv* original_env, ERL_NIF_TERM ref):
        callback(original_env, ref)
    { }
           
    void operator() (const std::vector<ioremap::elliptics::error_info>& errs)
    {
        // TODO: Make correct error reporting later
        bool result = true;
        for (auto it = errs.cbegin(); result && it != errs.cend(); ++it) {
            result = !(*it);
        }
        ERL_NIF_TERM r_term;
        if (result) {
            r_term = enif_make_copy(env(), atoms::OK);
        }
        else {
            r_term = enif_make_tuple2(env(), 
                    enif_make_copy(env(), atoms::ERROR), 
                    enif_make_copy(env(), atoms::BACKEND_ERROR));
        }
        send_reply(r_term);
    }
};

class data_callback: public callback
{
public:
    data_callback(ErlNifEnv* original_env, ERL_NIF_TERM ref):
        callback(original_env, ref)
    { }
        
    void operator() (const std::string& data)
    {
        ERL_NIF_TERM r_data_term;
        
        uint8_t * buff = enif_make_new_binary(env(), data.size(), &r_data_term);
        memcpy(buff, &data[0], data.size());
        
        ERL_NIF_TERM r_term = enif_make_tuple2(env(), atoms::DATA, r_data_term);
        send_reply(r_term);
    }
};
    
const char* erl_binding::MODULE = "relliptics_nif";
    
int erl_binding::load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    (void)load_info;
    
    atoms::LOG_PATH = enif_make_atom(env, "log_path");
    atoms::NODES = enif_make_atom(env, "nodes");
    atoms::GROUPS = enif_make_atom(env, "groups");
    atoms::NAMESPACE = enif_make_atom(env, "namespace");
    
    atoms::OK = enif_make_atom(env, "ok");
    atoms::ERROR = enif_make_atom(env, "error");
    atoms::BACKEND_ERROR = enif_make_atom(env, "backend_error");
    
    atoms::DATA = enif_make_atom(env, "data");
    
    atoms::ALL = enif_make_atom(env, "all");
    atoms::ANY = enif_make_atom(env, "any");
            
    try
    {
        *priv_data = new erl_binding(env);
        return 0;
    }
    catch(...)
    {
        return 1;
    }
}

erl_binding::erl_binding(ErlNifEnv* caller_env)
{
    _db_resource_type = enif_open_resource_type(caller_env, MODULE, "db_connection", 
                                                &erl_binding::remove_db_connection, ERL_NIF_RT_CREATE, 0);
    if (_db_resource_type == 0) {
        throw std::runtime_error("Can't create resource type");
    }
}

void erl_binding::unload(ErlNifEnv* env, void* priv_data)
{
    (void)env;
    delete static_cast<erl_binding*>(priv_data);
}

erl_binding& erl_binding::instance(ErlNifEnv* env)
{
    void * ptr = enif_priv_data(env);
    assert(ptr != 0);
    return *static_cast<erl_binding*>(ptr);
}

ERL_NIF_TERM erl_binding::make_ok_result(ErlNifEnv* env, ERL_NIF_TERM value)
{
    return enif_make_tuple2(env, atoms::OK, value);
}

struct erl_binding::db_connection
{
    std::shared_ptr<backend> inst;
};

void erl_binding::remove_db_connection(ErlNifEnv* env, void* obj)
{
    (void)env;
    static_cast<erl_binding::db_connection*>(obj)->~db_connection();
}

ERL_NIF_TERM erl_binding::open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 1);
    return instance(env).open(env, argv[0]);
}

ERL_NIF_TERM erl_binding::open(ErlNifEnv* env, ERL_NIF_TERM config)
{
    if (!enif_is_list(env, config)) {
        return enif_make_badarg(env);
    }
    
    std::string log_path = "/dev/stderr";
    std::vector<node_entry> nodes{{"localhost", 1025}};
    std::vector<int> groups{0};
    std::string ns;
    
    ERL_NIF_TERM item;
    try
    {
        while (!enif_is_empty_list(env, config)) {
            bool r = enif_get_list_cell(env, config, &item, &config);
            assert(r);
            if (!enif_is_tuple(env, item)) {
                return enif_make_badarg(env);
            }
            const ERL_NIF_TERM* property;
            int arity;
            r = enif_get_tuple(env, item, &arity, &property);
            assert(r);
            if (arity != 2) {
                return enif_make_badarg(env);
            }
            const ERL_NIF_TERM key = property[0];
            const ERL_NIF_TERM value = property[1];
            if (enif_compare(key, atoms::LOG_PATH)) {
                log_path = read_string(env, value);
            }
            else if (enif_compare(key, atoms::NODES)) {
                nodes = read_nodes(env, value);
            }
            else if (enif_compare(key, atoms::GROUPS)) {
                groups = read_groups(env, value);
            }
            else if (enif_compare(key, atoms::NAMESPACE)) {
                ns = read_string(env, value);
            }
        }
        std::shared_ptr<backend> db =  backend::create(log_path, nodes, groups, ns);
        void* db_ptr = enif_alloc_resource(_db_resource_type, sizeof(erl_binding::db_connection));
        new(db_ptr)erl_binding::db_connection({db});
        ERL_NIF_TERM conn_term = enif_make_resource(env, db_ptr);
        return make_ok_result(env, conn_term);
    }
    catch (...)
    {
        return enif_make_badarg(env);
    }
}

std::string erl_binding::read_string(ErlNifEnv* env, ERL_NIF_TERM value) const
{
    ErlNifBinary bin;
    if(!enif_inspect_binary(env, value, &bin)) {
        throw std::invalid_argument("can't read binary");
    }
    return std::string(reinterpret_cast<char*>(bin.data), bin.size);
}

std::vector<node_entry> erl_binding::read_nodes(ErlNifEnv* env, ERL_NIF_TERM value) const
{
    if (!enif_is_list(env, value)) {
        throw std::invalid_argument("nodes is not a list of nodes");
    }
    std::vector<node_entry> nodes;
    ERL_NIF_TERM node;
    while(enif_get_list_cell(env, value, &node, &value)) {
        if (enif_is_tuple(env, node)) {
            const ERL_NIF_TERM* addr_port;
            int arity;
            bool r = enif_get_tuple(env, node, &arity, &addr_port);
            assert(r);
            if (arity != 2) {
                throw std::invalid_argument("invalid node record");
            }
            const ERL_NIF_TERM addr_term = addr_port[0];
            std::string addr = read_string(env, addr_term);
            
            const ERL_NIF_TERM port_term = addr_port[1];
            int port;
            if (!enif_get_int(env, port_term, &port)) {
                throw std::invalid_argument("invalid port value");
            }
            
            nodes.emplace_back(addr, port);
        }
        else {
            std::string addr = read_string(env, node);
            nodes.emplace_back(addr);
        }
    }
    return nodes;
}

 std::vector<int> erl_binding::read_groups(ErlNifEnv* env, ERL_NIF_TERM value) const
 {
    if (!enif_is_list(env, value)) {
        throw std::invalid_argument("groups is not a list of groups");
    }
    std::vector<int> groups;
    ERL_NIF_TERM group_term;
    while(enif_get_list_cell(env, value, &group_term, &value)) {
        int group;
        if (!enif_get_int(env, group_term, &group)) {
            throw std::invalid_argument("group should be an integer");
        }
        groups.push_back(group);
    }
    return groups;
 }

ERL_NIF_TERM erl_binding::write_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 3);
    return instance(env).write_async(env, argv[0], argv[1], argv[2]);
}

ERL_NIF_TERM erl_binding::write_async(ErlNifEnv* env, ERL_NIF_TERM caller_ref, ERL_NIF_TERM db_ref, ERL_NIF_TERM data_term)
{
    if (!enif_is_list(env, data_term)) {
        return enif_make_badarg(env);
    }
    if (!enif_is_ref(env, caller_ref)) {
        return enif_make_badarg(env);
    }
    
    try
    {
        unsigned len;
        bool r = enif_get_list_length(env, data_term, &len);
        assert(r);
        std::vector<backend_object> data;
        data.reserve(len);
        ERL_NIF_TERM item;
        while (enif_get_list_cell(env, data_term, &item, &data_term)) {
            int arity;
            const ERL_NIF_TERM* object_data;
            if (!enif_get_tuple(env, item, &arity, &object_data)) {
                return enif_make_badarg(env);
            }
            if (arity < 2 || arity > 4) {
                return enif_make_badarg(env);
            }
            std::string key = read_string(env, object_data[0]);
            std::string value = read_string(env, object_data[1]);
            std::vector<std::string> indxs_to_save;
            if (arity > 2) {
                indxs_to_save = read_stringlist(env, object_data[2]);
            }
            std::vector<std::string> indxs_to_remove;
            if (arity > 3) {
                indxs_to_remove = read_stringlist(env, object_data[3]);
            }
            data.emplace_back(key, value, indxs_to_save, indxs_to_remove);
        }
        db_connection& conn = get_connection(env, db_ref);
        conn.inst->write(data, complete_callback(env, caller_ref));
        return atoms::OK;
    }
    catch (const std::invalid_argument&)
    {
        return enif_make_badarg(env);
    }
}

std::vector<std::string> erl_binding::read_stringlist(ErlNifEnv* env, ERL_NIF_TERM value) const
{
    if (!enif_is_list(env, value)) {
        throw std::invalid_argument("is not a list");
    }
    unsigned len;
    bool r = enif_get_list_length(env, value, &len);
    assert(r);
    std::vector<std::string> stringlist;
    stringlist.reserve(len);
    ERL_NIF_TERM term;
    while(enif_get_list_cell(env, value, &term, &value)) {
        stringlist.push_back(read_string(env, term));
    }
    return stringlist;
}

ERL_NIF_TERM erl_binding::read_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 3);
    return instance(env).read_async(env, argv[0], argv[1], argv[2]);
}

ERL_NIF_TERM erl_binding::read_async(ErlNifEnv* env, ERL_NIF_TERM caller_ref, ERL_NIF_TERM db_ref, ERL_NIF_TERM keys_term)
{
    if (!enif_is_ref(env, caller_ref)) {
        return enif_make_badarg(env);
    }
    try
    {
        std::vector<std::string> keys = read_stringlist(env, keys_term);
        db_connection& conn = get_connection(env, db_ref);
        conn.inst->read(keys, data_callback(env, caller_ref), complete_callback(env, caller_ref));
        return atoms::OK;
    }
    catch (const std::invalid_argument&)
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM erl_binding::read_by_indexes_async(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 4);
    return instance(env).read_by_indexes_async(env, argv[0], argv[1], argv[2], argv[3]);
}

ERL_NIF_TERM erl_binding::read_by_indexes_async(ErlNifEnv* env, ERL_NIF_TERM caller_ref, ERL_NIF_TERM db_ref, 
                                                ERL_NIF_TERM strategy_term, ERL_NIF_TERM indexes_term)
{
    if (!enif_is_ref(env, caller_ref)) {
        return enif_make_badarg(env);
    }
    INDEX_SEARCH_STRATEGY strategy;
    if (enif_compare(strategy_term, atoms::ALL) == 0) {
        strategy = INDEX_SEARCH_STRATEGY::ALL;
    }
    else if (enif_compare(strategy_term, atoms::ANY) == 0) {
        strategy = INDEX_SEARCH_STRATEGY::ANY;
    }
    else {
        return enif_make_badarg(env);
    }
    
    try
    {
        std::vector<std::string> indexes = read_stringlist(env, indexes_term);
        db_connection& conn = get_connection(env, db_ref);
        conn.inst->read_by_indexes(strategy, indexes, data_callback(env, caller_ref), complete_callback(env, caller_ref));
        return atoms::OK;
    }
    catch (const std::invalid_argument&)
    {
        return enif_make_badarg(env);
    }
}

ERL_NIF_TERM erl_binding::close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    assert(argc == 1);
    return instance(env).close(env, argv[0]);
}

ERL_NIF_TERM erl_binding::close(ErlNifEnv* env, ERL_NIF_TERM db_ref)
{
    try
    {
        db_connection& conn = get_connection(env, db_ref);
        if (conn.inst == 0) {
            return enif_make_badarg(env);
        }
        conn.inst.reset();
        return atoms::OK;
    }
    catch (const std::invalid_argument&)
    {
        return enif_make_badarg(env);
    }
}

erl_binding::db_connection& erl_binding::get_connection(ErlNifEnv* env, ERL_NIF_TERM ref)
{
    void * ptr;
    if(!enif_get_resource(env, ref, _db_resource_type, &ptr)) {
        throw std::invalid_argument("Invalid db ref");
    }
    else {
        return *static_cast<db_connection*>(ptr);
    }
}

} //namespace

static ErlNifFunc nif_funcs[] = 
{
    {"open", 1, relliptics::erl_binding::open},
    {"write_async", 3, relliptics::erl_binding::write_async},
    {"read_async", 3, relliptics::erl_binding::read_async},
    {"read_by_indexes_async", 4, relliptics::erl_binding::read_by_indexes_async},
    {"close", 1, relliptics::erl_binding::close}
};

ERL_NIF_INIT(relliptics_nif, nif_funcs, &relliptics::erl_binding::load, 0, 0, &relliptics::erl_binding::unload)
