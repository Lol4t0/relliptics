/* 
 * File:   backend.h
 * Author: sidorov
 *
 * Created on January 28, 2014, 12:45 PM
 */

#ifndef BACKEND_H
#define	BACKEND_H

#include <utility>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <stdexcept>

namespace ioremap
{
namespace elliptics
{
    
    class node;
    class session;
    class error_info;
    class key;
    class read_result_entry;
}
}

namespace relliptics
{
    
namespace exceptions
{
    class bad_node_exception: public std::runtime_error
    {
    public:
        bad_node_exception():
        runtime_error("Bad node config")
        {}
    };
}

struct node_entry
{
    node_entry(std::string name, int port = 1025):
        name(std::move(name)), 
        port(port)
    {}
    std::string name;
    int port;
};

struct backend_object
{
    backend_object(std::string key, std::string value, 
                   std::vector<std::string> indexes_to_save = std::vector<std::string>(), 
                   std::vector<std::string> indexes_to_remove = std::vector<std::string>()):
        key(std::move(key)),
        object_data(std::move(value)),
        indexes_to_save(std::move(indexes_to_save)),
        indexes_to_remove(std::move(indexes_to_remove))
    {}
    std::string key;
    std::string object_data;

    std::vector<std::string> indexes_to_save;
    std::vector<std::string> indexes_to_remove;
};

typedef std::function<void(std::vector<ioremap::elliptics::error_info>)> completion_handler;
typedef std::function<void(std::string)> data_handler;

enum class INDEX_SEARCH_STRATEGY
{
    ALL,
    ANY
};

class backend: public std::enable_shared_from_this<backend> {
    friend class read_by_indexes_watcher;
public:
    backend(const std::string& log_path, const std::vector<node_entry>& nodes, 
            const std::vector<int>& groups, const std::string& ns);
    
    static std::shared_ptr<backend> create(const std::string& log_path, const std::vector<node_entry>& nodes, 
                                           const std::vector<int>& groups, const std::string& ns);
    ~backend();
    
    void write(std::vector<backend_object> data, completion_handler handler);
    void read(const std::vector<std::string>& keys, data_handler data_hndlr, completion_handler cmpl_hndlr);
    void read_by_indexes(INDEX_SEARCH_STRATEGY strategy, 
                         const std::vector<std::string>& indexes, data_handler data_hndlr, completion_handler cmpl_hndlr);
    
private:
    
    
    std::unique_ptr<ioremap::elliptics::node> _node;
    std::unique_ptr<ioremap::elliptics::session> _session;
    std::vector<int> _groups;
    
    static void read_data_hndlr(data_handler data_hndlr, const ioremap::elliptics::read_result_entry& item);
    static void single_result_cmpl_hndlr(completion_handler cmpl_hndlr, const ioremap::elliptics::error_info& e);
    
    inline std::weak_ptr<backend> ptr();
    

};

}

#endif	/* BACKEND_H */

