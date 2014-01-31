/*
 * File:   backend_test.cpp
 * Author: sidorov
 *
 * Created on Jan 29, 2014, 4:20:56 PM
 */

#include <elliptics/error.hpp>

#include "backend_test.h"

#include <mutex>
#include <condition_variable>

CPPUNIT_TEST_SUITE_REGISTRATION(backend_test);

backend_test::backend_test() 
{
}

backend_test::~backend_test() 
{
}

void backend_test::setUp() 
{
    std::vector<relliptics::node_entry> nodes;
    nodes.emplace_back("localhost", 1025);
    backend = relliptics::backend::create("/dev/stderr", nodes, std::vector<int>{0}, "backend_test");
}

void backend_test::tearDown() 
{
}

template<typename T>
class back_reporter
{
public:
    back_reporter():completed(false){}
    
    void operator() (const T& t)
    {
        std::lock_guard<std::mutex> locker(m);
        completed = true;
        data = t;
        cond.notify_all();
    }
    
    T get()
    {
        std::unique_lock<std::mutex> locker(m);
        while (!completed) {
            cond.wait_for(locker, std::chrono::milliseconds(120));
        }
        return data;
    }
    
    bool is_empty() const
    {
        return !completed;
    }
private:
    T data;
    bool completed;
    std::mutex m;
    std::condition_variable cond;
    
};

void backend_test::test_write() 
{
    std::vector<relliptics::backend_object> objs;
    objs.emplace_back("simple_key", "value");
    objs.emplace_back("indexes_key", "super value", std::vector<std::string>{"index_1"});
    back_reporter<std::vector<ioremap::elliptics::error_info>> r;
    backend->write(objs, std::ref(r));
    std::vector<ioremap::elliptics::error_info> ers = r.get();
    CPPUNIT_ASSERT(ers.size() == 2);
    CPPUNIT_ASSERT(!ers[0]);    
    CPPUNIT_ASSERT(!ers[1]);

}

void backend_test::test_read_by_key()
{
    std::vector<std::string> keys;
    keys.emplace_back("simple_key");
    
    back_reporter<std::string> data_r;
    back_reporter<std::vector<ioremap::elliptics::error_info>> stat_r;
    backend->read(keys, std::ref(data_r), std::ref(stat_r));
    std::string data = data_r.get();
    CPPUNIT_ASSERT(data == "value");
    std::vector<ioremap::elliptics::error_info> ers = stat_r.get();
    CPPUNIT_ASSERT(ers.size() == 1);
    CPPUNIT_ASSERT(!ers[0]);
}

void backend_test::test_read_by_index() 
{
    std::vector<std::string> keys;
    keys.emplace_back("index_1");
    
    back_reporter<std::string> data_r;
    back_reporter<std::vector<ioremap::elliptics::error_info>> stat_r;
    backend->read_by_indexes(relliptics::INDEX_SEARCH_STRATEGY::ALL, keys, std::ref(data_r), std::ref(stat_r));
    std::string data = data_r.get();
    CPPUNIT_ASSERT(data == "super value");
    std::vector<ioremap::elliptics::error_info> ers = stat_r.get();
    CPPUNIT_ASSERT(ers.size() == 1);
    CPPUNIT_ASSERT(!ers[0]);
}

void backend_test::test_read_by_index_empty() 
{
    std::vector<std::string> keys;
    keys.emplace_back("index_1", "index_not_exists");
    
    back_reporter<std::string> data_r;
    back_reporter<std::vector<ioremap::elliptics::error_info>> stat_r;
    backend->read_by_indexes(relliptics::INDEX_SEARCH_STRATEGY::ALL, keys, std::ref(data_r), std::ref(stat_r));
    std::vector<ioremap::elliptics::error_info> ers = stat_r.get();
    CPPUNIT_ASSERT(ers.size() == 1);
    CPPUNIT_ASSERT(!ers[0]);
    sleep(1.0);
    CPPUNIT_ASSERT(data_r.is_empty());
}

