#ifndef __DOWNPOUR__WORKER__H__
#define __DOWNPOUR__WORKER__H__

#include <cstring>
#include <string>
#include <unistd.h>

#include "abstract_work_table.h"
#include "ib/run.h"

using namespace std;

namespace downpour {

class Worker {
public:
	Worker(AbstractWorkTable* work_table) : _work_table(work_table) {}

	virtual void work() {
		bool filled = false;
		while (true) {
			size_t row;
			size_t col;
			string what;
			string data;
			_work_table->get_work(&row, &col, &what, &data);
			if (row == -1) {
				string result;
				Run run(what, data);
				run();
				Logger::info("..");
				stringstream ss;
				ss << run.read();
				Logger::info("--%", ss.str());
				while (ss.good()) {
					string entry;
					getline(ss, entry);
					_work_table->done_work(-1, 0, entry);
				}
				if (!filled) {
					filled = true;
					continue;
				}
				break;
				// TODO: check if anything was added.
			}

			string result;
			int error = do_work(row, col, what, data, &result);
			Logger::info("result % %", error, result);
		}
	}

	virtual int do_work(size_t row, size_t col, const string& what,
			    const string& data, string* result) {
		Logger::info("doing work for % %: %", row, col, what);
		Run run(what, data);
		run();

		// TODO: check for errors
		*result = run.read();
		return 0;
	}

protected:
	AbstractWorkTable* _work_table;
};

}  // namespace downpour

#endif  // __DOWNPOUR__WORKER__H__
