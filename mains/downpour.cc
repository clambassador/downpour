#include <csignal>
#include <string>
#include <vector>
#include <unistd.h>

#include "ib/fileutil.h"
#include "ib/logger.h"
#include "downpour/abstract_work_table.h"
#include "downpour/work_table.h"
#include "downpour/worker.h"

using namespace std;
using namespace downpour;
using namespace ib;

unique_ptr<AbstractWorkTable> _table;

void sigterm_handler(int s) {
	Logger::info("sigterm: %", s);
	_table.reset(nullptr);
	exit(0);
}

void sigignore_handler(int s) {}

int main(int argc, char** argv) {
	if (argc < 3) {
		Logger::error("usage: downpour format savefile num_workers workernames args");
		exit(1);
	}
	string format(argv[1]);
	string outfile(argv[2]);
	if (Fileutil::is_newer(format, outfile)) unlink(outfile.c_str());

	size_t num_workers = atoi(argv[3]);
	assert(num_workers);
	assert(argc >= 3 + num_workers);

	Logger::info("(downpour) using % workers", num_workers);
	vector<string> params;
	for (size_t i = 4 + num_workers; i < argc; ++i) {
		params.push_back(argv[i]);
	}

        signal(SIGTERM, sigterm_handler);
        signal(SIGINT, sigterm_handler);
        signal(SIGALRM, sigignore_handler);

	_table.reset(new WorkTable(format, outfile, params));
	_table->load();
	vector<unique_ptr<Worker>> workers;
	for (size_t i = 0; i < num_workers; ++i) {
		workers.push_back(nullptr);
		workers.back().reset(new Worker(_table.get(), argv[4 + i], i + 1));
		workers.back()->start();
	}

	for (auto& x : workers) {
		x->join();
	}

	Logger::info("(downpour) worker finished; normal exit.");
	_table.reset(nullptr);
	return 0;
}
