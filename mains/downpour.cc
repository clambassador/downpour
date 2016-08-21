#include "ib/logger.h"
#include "downpour/abstract_work_table.h"
#include "downpour/work_table.h"
#include "downpour/worker.h"

#include <csignal>
#include <string>
#include <vector>
#include <unistd.h>

using namespace std;
using namespace downpour;
using namespace ib;

unique_ptr<AbstractWorkTable> _table;

void sigterm_handler(int s) {
	Logger::info("sigterm: %", s);
	_table->save();
	exit(0);
}

void sigchild_handler(int s) {
	Logger::info("(downpour worker): child exit");
}

void sigignore_handler(int s) {}

int main(int argc, char** argv) {
	if (argc < 3 || argc > 3) {
		Logger::error("usage: downpour format savefile");
		exit(1);
	}
	string format(argv[1]);
	string outfile(argv[2]);

        signal(SIGCHLD, sigchild_handler);
        signal(SIGTERM, sigterm_handler);
        signal(SIGINT, sigterm_handler);
        signal(SIGALRM, sigignore_handler);

	_table.reset(new WorkTable(format, outfile));
	_table->load();
	Worker worker(_table.get());

	worker.work();
	Logger::info("(downpour) worker finished; normal exit.");
	_table.reset(nullptr);
	return 0;
}
