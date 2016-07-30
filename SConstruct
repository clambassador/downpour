import os
print ""
print ""
print ""
print ""
print ""
print ""
print ""
print ""
print ""
print ""
common = Split("""logger.cc
	       """)
tests = dict()
tests['test_general.cc'] = 'test_general'

libs = Split("""
	     """)
#env = Environment(CXX="ccache clang++ -D_GLIBCXX_USE_NANOSLEEP 		  -D_GLIBCXX_USE_SCHED_YIELD -D_GLIBCXX_GTHREAD_USE_WEAK=0		  -Qunused-arguments -fcolor-diagnostics -I.. -I/usr/include/c++/4.7/ 		  -I/usr/include/x86_64-linux-gnu/c++/4.7/", 		  CPPFLAGS="-D_FILE_OFFSET_BITS=64 -Wall -g --std=c++11 -pthread", LIBS=libs, CPPPATH=".")
env = Environment(CXX="ccache clang++ -I..", CPPFLAGS="-D_FILE_OFFSET_BITS=64 -Wall -g --std=c++11 -pthread", LIBS=libs, CPPPATH="..")
env['ENV']['TERM'] = 'xterm'


Decider('MD5')
for i in tests:
	env.Program(source = ['tests/' + i] + common, target = tests[i])



