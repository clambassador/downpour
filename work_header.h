#ifndef __DOWNPOUR__WORK_HEADER__H__
#define __DOWNPOUR__WORK_HEADER__H__

#include <fstream>
#include <set>
#include <string>
#include <vector>

#include "ib/tokenizer.h"

using namespace std;
using namespace ib;

namespace downpour {

class WorkHeader {
public:
	enum {
		RAW,
		NEWLINE,
		STRING,
		VECTOR
	} style_t;

	WorkHeader() {}

	virtual ~WorkHeader() {}

	virtual void init(const string& filename, const vector<string>& params) {
		ifstream fin(filename);
		assert(fin.good());

		while (fin.good()) {
			string column_name, program, args;
			while (column_name.empty()) {
				getline(fin, column_name);
				if (!fin.good()) goto exit;
			}
			getline(fin, program);
			for (size_t i = 0; i < params.size(); ++i) {
				string pre = "%" + Logger::stringify("%", i + 1);
				program = Tokenizer::replace(program, pre,
							     params[i]);
			}
			getline(fin, args);
			assert(program.size());
			_column_args.push_back(vector<size_t>());
			_column_args_style.push_back(RAW);
			if (args.size()) {
				if (args[0] == 'v') {
					args = args.substr(1);
					_column_args_style.pop_back();
					_column_args_style.push_back(VECTOR);
				} else if (args[0] == 's') {
					args = args.substr(1);
					_column_args_style.pop_back();
					_column_args_style.push_back(STRING);
				} else if (args[0] == 'n') {
					args = args.substr(1);
					_column_args_style.pop_back();
					_column_args_style.push_back(NEWLINE);
				}

				stringstream ss;
				ss << args;

				while (ss.good()) {
					size_t arg;
					set<size_t> test;
					ss >> arg;
					assert(arg < _column_names.size());
					assert(!test.count(arg));
					test.insert(arg);
					_column_args.back().push_back(arg);
				}
			}

			assert(column_name.size());
			assert(program.size());
			_column_names.push_back(column_name);
			_column_programs.push_back(program);

			_column_waitfor = _column_args;
			set<size_t> cols;
			for (auto &x : _column_args.back())cols.insert(x);
			for (int i = 0; i < _column_names.size(); ++i) {
				if (cols.count(i)) continue;
				if (program.find(Logger::stringify("$%", i)) != string::npos ||
				    program.find(Logger::stringify("$(%)", i)) != string::npos) {
					cols.insert(i);
				}
			}
		}
exit:
		Logger::info("(downpour) Header: %", _column_names);
		Logger::info("(downpour) Programs: %", _column_programs);
	}

	virtual void get_work(size_t col,
			      string* what,
			      vector<size_t>* column_args,
			      int* arg_style) const {
		assert(what);
		assert(column_args);

		*what = _column_programs.at(col);
		*column_args = _column_args.at(col);
		*arg_style = _column_args_style.at(col);
	}

	virtual size_t columns() const {
		return _column_args.size();
	}

	virtual void get_column_args(size_t col, vector<size_t>* args) const {
		*args = _column_args[col];
	}

	virtual void get_column_waitfor(size_t col, vector<size_t>* waitfor) const {
		*waitfor = _column_waitfor[col];
	}

	virtual int get_column_args_style(size_t col) const {
		return _column_args_style.at(col);
	}

	virtual string get_column_name(size_t col) const {
		return _column_names[col];
	}

protected:
	vector<string> _column_names;
	vector<string> _column_programs;
	vector<vector<size_t>> _column_args;
	vector<vector<size_t>> _column_waitfor;
	vector<int> _column_args_style;
};

}

#endif  //  __DOWNPOUR__WORK_TABLE__H__

