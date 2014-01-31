/*
 * File:   backend_test.h
 * Author: sidorov
 *
 * Created on Jan 29, 2014, 4:20:55 PM
 */

#ifndef BACKEND_TEST_H
#define	BACKEND_TEST_H

#include "backend.h"
#include <cppunit/extensions/HelperMacros.h>


class backend_test : public CPPUNIT_NS::TestFixture {
    CPPUNIT_TEST_SUITE(backend_test);

    CPPUNIT_TEST(test_write);
    CPPUNIT_TEST(test_read_by_key);
    CPPUNIT_TEST(test_read_by_index);
    CPPUNIT_TEST(test_read_by_index_empty);

    CPPUNIT_TEST_SUITE_END();

public:
    backend_test();
    virtual ~backend_test();
    void setUp();
    void tearDown();

private:
    void test_write();
    void test_read_by_key();
    void test_read_by_index();
    void test_read_by_index_empty();
    
    std::shared_ptr<relliptics::backend> backend;
};

#endif	/* BACKEND_TEST_H */

