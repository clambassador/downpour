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
	_table->save();
	exit(0);
}

void sigignore_handler(int s) {}

int main(int argc, char** argv) {
	if (argc < 3) {
		Logger::error("usage: downpour format savefile args");
		exit(1);
	}
	string format(argv[1]);
	string outfile(argv[2]);
	if (Fileutil::is_newer(format, outfile)) unlink(outfile.c_str());
	vector<string> params;
	for (size_t i = 3; i < argc; ++i) {
		params.push_back(argv[i]);
	}

        signal(SIGTERM, sigterm_handler);
        signal(SIGINT, sigterm_handler);
        signal(SIGALRM, sigignore_handler);

	_table.reset(new WorkTable(format, outfile, params));
	_table->load();
	Worker worker(_table.get());

	worker.work();
	Logger::info("(downpour) worker finished; normal exit.");
	_table.reset(nullptr);
	return 0;
}
