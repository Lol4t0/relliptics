
all: compile

get-deps:
		./c_src/build_deps.sh get-deps

rm-deps:
		./c_src/build_deps.sh rm-deps

compile:
		./rebar compile

test: compile c_test
		./rebar eunit

clean:
		./rebar clean

c_test: c_test_run

c_test_compile:
	g++ -g -Wall -Wextra -std=c++0x c_src/tests/*.cpp c_src/backend.cpp -Ic_src -Ic_include -Lpriv -Wl,-rpath,priv -lelliptics_client -lelliptics_cpp -opriv/cpp_test `cppunit-config --cflags` `cppunit-config --libs`

c_test_run: c_test_compile
	LD_LIBRARY_PATH=priv priv/cpp_test

