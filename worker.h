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
		while (true) {
			size_t row;
			size_t col;
			string what;
			vector<string> data;
			_work_table->get_work(&row, &col, &what, &data);
			if (row == -1) {
				// TODO: do the create more work
				break;
			}

			string result;
			int error = do_work(row, col, what, data, &result);
			Logger::info("result % %", error, result);
		}
	}

	virtual int do_work(size_t row, size_t col, const string& what,
			    const vector<string>& data, string* result) {
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
