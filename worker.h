#ifndef __DOWNPOUR__WORKER__H__
#define __DOWNPOUR__WORKER__H__

#include <cstring>
#include <memory>
#include <string>
#include <thread>
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
			string data;
			_work_table->get_work(&row, &col, &what, &data);
			if (row == -1) {
				string result;
				assert(what.length());
				Run run(what, data);
				run();
				stringstream ss;
				ss << run.read();
				bool novel = false;
				while (ss.good()) {
					string entry;
					getline(ss, entry);
					if (entry.empty()) continue;
					novel |= _work_table->done_work(-1, 0, entry);
				}
				Logger::info("(downpour worker) added % work",
					     novel ? "novel" : "no novel");
				if (!novel) break;
			} else {
				string result;
				int error = do_work(row, col, what, data, &result);
				if (error) {
					_work_table->error(row, col, result);
				} else {
					_work_table->done_work(row, col,
							       result);
				}
			}
		}
	}

	virtual int do_work(size_t row, size_t col, const string& what,
			    const string& data, string* result) {
		Logger::info("(downpour worker) doing work for % %: %", row, col, what);
		bool redirect = false;
		string cmd;
		Logger::info("% %", what, what[what.length() - 1]);
		if (what[what.length() - 1] == '>') {
			cmd = what.substr(0, what.length() - 1);
			redirect = true;
		} else {
			cmd = what;
		}
		Run run(cmd, data);
		run();

		// TODO: check for errors
		if (redirect) {
			string filename = Logger::stringify(
				"./%__data__%__%",
				_work_table->name(), row, col);
			run.redirect(filename);
			*result = filename;
		} else {
			*result = run.read();
		}
		return 0;
	}

	virtual void spawn() {
		_thread.reset(new thread(
			&Worker::work, this));
	}

protected:
	AbstractWorkTable* _work_table;
	unique_ptr<thread> _thread;
};

}  // namespace downpour

#endif  // __DOWNPOUR__WORKER__H__
