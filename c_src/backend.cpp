/* 
 * File:   backend.cpp
 * Author: sidorov
 * 
 * Created on January 28, 2014, 12:45 PM
 */

#include "backend.h"
#include <utility>
#include <tuple>
#include <cstdatomic>

#include <elliptics/session.hpp>
#include <assert.h>
#include <iostream>


namespace relliptics
{

backend::backend(const std::string& log_path, const std::vector<node_entry>& nodes, 
        const std::vector<int>& groups, const std::string& ns):
        _node(new ioremap::elliptics::node(ioremap::elliptics::file_logger(log_path.c_str(), DNET_LOG_ERROR))),
        _session(new ioremap::elliptics::session(*_node)),
        _groups(groups)
{
    if (!_node->is_valid()) {
        throw exceptions::bad_node_exception();
    }
    for (auto it = nodes.cbegin(); it != nodes.cend(); ++it) {
        const node_entry& n = *it;
        _node->add_remote(n.name.c_str(), n.port);
    }
    
    _session->set_groups(groups);
    _session->set_namespace(ns.c_str(), ns.size());
}

std::shared_ptr<backend> backend::create(const std::string& log_path, const std::vector<node_entry>& nodes, 
                                         const std::vector<int>& groups, const std::string& ns)
{
    return std::make_shared<backend>(log_path, nodes, groups, ns);
}

std::weak_ptr<backend> backend::ptr()
{
    return shared_from_this();
}

class write_callback
{
public:
    
    write_callback(int total, completion_handler final_handler):
        _total(total),
        _progress(0),
        _final_handler(final_handler)
    {
        _status.resize(total);
    }
        
    void finish_chunk(const ioremap::elliptics::error_info& e)
    {
        int indx = _progress.fetch_add(1,std::memory_order_relaxed);
        _status[indx] = e;
        if (indx == _total - 1) {
            _final_handler(std::move(_status));
            _final_handler = 0;
        }

    }
    
    ~write_callback()
    {
        assert(_final_handler == 0);
    }

private:
    int _total;
    std::atomic_int _progress;
    std::vector<ioremap::elliptics::error_info> _status;

    completion_handler _final_handler;
};

void backend::write(std::vector<backend_object> data, completion_handler handler)
{
    assert(data.size() != 0);

    int total_size = 1; // for data
    
    //prepare data & indexes
    std::vector<dnet_io_attr> keys(data.size());
    std::vector<std::string> datas(data.size());
    
    std::vector<std::pair<ioremap::elliptics::key, std::vector<std::string>>> indexes_to_save;
    indexes_to_save.reserve(data.size());
    std::vector<std::pair<ioremap::elliptics::key, std::vector<std::string>>> indexes_to_remove;
    indexes_to_remove.reserve(data.size());
    
    for (size_t k = 0; k < data.size(); ++k) {
        backend_object& obj = data[k];

        dnet_io_attr& attr = keys[k];
        ioremap::elliptics::key key(obj.key);
        key.transform(*_session);
        memcpy(attr.id, key.raw_id().id, sizeof(attr.id));
        datas[k] = std::move(obj.object_data);

        if (!obj.indexes_to_save.empty()) {
            total_size ++;
            indexes_to_save.emplace_back(key, std::move(obj.indexes_to_save));
        }
        if (!obj.indexes_to_remove.empty()) {
            total_size++;
            indexes_to_remove.emplace_back(key, std::move(obj.indexes_to_remove));
        }
        
    }
    
    std::shared_ptr<write_callback> cb = std::make_shared<write_callback>(total_size, std::move(handler));
    auto hndlr = std::bind(&write_callback::finish_chunk, cb, std::placeholders::_1);
    
    _session->bulk_write(keys, datas).connect(0, hndlr);
    
    for (auto it = indexes_to_save.cbegin(); it != indexes_to_save.cend(); ++it) {
        size_t indxs_size = it->second.size();
        _session->update_indexes(it->first, it->second, std::vector<ioremap::elliptics::data_pointer>(indxs_size))
                .connect(0, hndlr);
    }
    
    for (auto it = indexes_to_remove.cbegin(); it != indexes_to_remove.cend(); ++it) {
        _session->remove_indexes(it->first, it->second).connect(0, hndlr);
    }
}

void backend::read(const std::vector<std::string>& keys, data_handler data_hndlr, completion_handler cmpl_hndlr)
{
    _session->bulk_read(keys).connect(
        std::bind(&backend::read_data_hndlr, data_hndlr, std::placeholders::_1), 
        std::bind(&backend::single_result_cmpl_hndlr, cmpl_hndlr, std::placeholders::_1));
}

void backend::read_data_hndlr(data_handler data_hndlr, const ioremap::elliptics::read_result_entry& item)
{
    data_hndlr(item.file().to_string());
}

void backend::single_result_cmpl_hndlr(completion_handler cmpl_hndlr, const ioremap::elliptics::error_info& e)
{
    return cmpl_hndlr(std::vector<ioremap::elliptics::error_info>{e});
}

class read_by_indexes_watcher: public std::enable_shared_from_this<read_by_indexes_watcher>
{
public:
    read_by_indexes_watcher(std::weak_ptr<backend> backend, data_handler data_hndlr, completion_handler cmpl_hndlr):
        _backend(backend),
        _data_hndlr(data_hndlr),
        _cmpl_hndlr(cmpl_hndlr),
                
        _data_in_progress(0),
        _indexes_completed(false),
                
        _complete_been_run(false),
        _error_occured(false)
    {}
        
    void process_index(const ioremap::elliptics::find_indexes_result_entry& item)
    {
        std::shared_ptr<backend> ptr = _backend.lock();
        if (ptr) {
            ioremap::elliptics::key(item.id);
            dnet_io_attr attr = dnet_io_attr();
            memcpy(attr.id, item.id.id, sizeof(attr.id));
            _data_in_progress.fetch_add(1, std::memory_order_relaxed);
            ptr->_session->read_data(item.id, ptr->_groups, attr).connect(
                std::bind(&read_by_indexes_watcher::process_data, shared_from_this(), std::placeholders::_1),
                std::bind(&read_by_indexes_watcher::finish_data, shared_from_this(), std::placeholders::_1));
        }
        else {
            complete();
        }
    }
    
    void finish_indexes(const ioremap::elliptics::error_info& e)
    {
        if (e) {
            _error_occured = true;
        }
        _indexes_completed = true;
        if (_data_in_progress.load(std::memory_order_relaxed) == 0) {
            complete();
        }
    }
    
    void process_data(const ioremap::elliptics::read_result_entry& item)
    {
        _data_hndlr(item.file().to_string());
    }
    
    void finish_data(const ioremap::elliptics::error_info& e)
    {
        if (e) {
            _error_occured = true;
        }
        if (_indexes_completed && 
            _data_in_progress.fetch_sub(1, std::memory_order_relaxed) == 1) {
            complete();
        }
    }
    
    ~read_by_indexes_watcher()
    {
        if(!_complete_been_run) {
            complete();
	}
    }
    
private:
    
    void complete()
    {
        bool exp = false;
        if (_complete_been_run.compare_exchange_strong(exp, true)) {
            auto err = _error_occured? ioremap::elliptics::error_info(-1, "some error") : ioremap::elliptics::error_info();
            _cmpl_hndlr(std::vector<ioremap::elliptics::error_info>{std::move(err)});
        }
    }

    std::weak_ptr<backend> _backend;

    data_handler _data_hndlr;
    completion_handler _cmpl_hndlr;
    
    std::atomic_int _data_in_progress;
    std::atomic_bool _indexes_completed;
    
    std::atomic_bool _complete_been_run;
    bool _error_occured;
    
};

void backend::read_by_indexes(INDEX_SEARCH_STRATEGY strategy, 
                              const std::vector<std::string>& indexes, data_handler data_hndlr, completion_handler cmpl_hndlr)
{
    std::shared_ptr<read_by_indexes_watcher> w 
            = std::make_shared<read_by_indexes_watcher>(this->ptr(), data_hndlr, cmpl_hndlr);
    switch (strategy) {
        case INDEX_SEARCH_STRATEGY::ALL:
            _session->find_all_indexes(indexes).connect(
                std::bind(&read_by_indexes_watcher::process_index, w, std::placeholders::_1),
                std::bind(&read_by_indexes_watcher::finish_indexes, w, std::placeholders::_1));
            break;
        case INDEX_SEARCH_STRATEGY::ANY:
            _session->find_any_indexes(indexes).connect(
                std::bind(&read_by_indexes_watcher::process_index, w, std::placeholders::_1),
                std::bind(&read_by_indexes_watcher::finish_indexes, w, std::placeholders::_1));
            break;
    }
    
}

backend::~backend()
{}


}

