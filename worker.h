#ifndef __DOWNPOUR__WORKER__H__
#define __DOWNPOUR__WORKER__H__

#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

#include "abstract_work_table.h"
#include "ib/run.h"
#include "ib/tokenizer.h"

using namespace std;

namespace downpour {

class Worker {
public:
	Worker(AbstractWorkTable* work_table, const string& name, size_t number)
	    : _work_table(work_table), _name(name), _number(number) {}
	virtual ~Worker() {}

	virtual void work() {
		unique_lock<mutex> ul(_mutex);

		while (!_work_table->exhausted()) {
			size_t row;
			size_t col;
			string what;
			string data;
			_work_table->get_work(_name, _number, &row, &col, &what, &data);
			what = Tokenizer::replace(what, "$NAME", _name);
			what = Tokenizer::replace(what, "$NUMBER",
						  Logger::stringify(_number));
			Logger::info("me % row % col % what % data %", _name,
				     (int64_t) row, col,what, data);
			if (row == -1) {
				if (col == -1) {
					sleep(1);
					continue;
				}
				string result;
				assert(what.length());
				Run run(what, data);
				run();
				stringstream ss;
				ss << run.read(30);
				bool novel = false;
				while (ss.good()) {
					string entry;
					getline(ss, entry);
					if (entry.empty()) continue;
					novel |= _work_table->done_work(-1, 0, entry);
				}
				Logger::info("(downpour worker) added % work",
					     novel ? "novel" : "no novel");
//				if (!novel) ;
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
		bool redirect = false;
		string cmd;
		if (what[what.length() - 1] == '>') {
			cmd = what.substr(0, what.length() - 1);
			redirect = true;
		} else {
			cmd = what;
		}
		Logger::info("(downpour worker) doing work for % %: %", row, col, what);
		Run run(cmd, data);
		run();

		if (redirect) {
			string filename = Logger::stringify(
				"./%__data__%__%",
				_work_table->name(), row, col);
			run.redirect(filename);
			*result = filename;
		} else {
			*result = run.read();
		}
		return run.result();
	}

	virtual void start() {
		_thread.reset(new thread(
			&Worker::work, this));
	}

	virtual void join() {
		_thread->join();
	}

protected:
	AbstractWorkTable* _work_table;
	string _name;
	size_t _number;
	unique_ptr<thread> _thread;
	mutex _mutex;
};

}  // namespace downpour

#endif  // __DOWNPOUR__WORKER__H__
