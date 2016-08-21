#ifndef __DOWNPOUR__WORK_HEADER__H__
#define __DOWNPOUR__WORK_HEADER__H__

#include <fstream>
#include <set>
#include <string>
#include <vector>

using namespace std;

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

	virtual void init(const string& filename) {
		ifstream fin(filename);
		assert(fin.good());

		while (fin.good()) {
			string column_name, program, args;
			while (column_name.empty()) {
				getline(fin, column_name);
				if (!fin.good()) goto exit;
			}
			getline(fin, program);
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
					assert(arg <= _column_names.size());
					assert(!test.count(arg));
					test.insert(arg);
					_column_args.back().push_back(arg);
				}
			}

			assert(column_name.size());
			assert(program.size());
			_column_names.push_back(column_name);
			_column_programs.push_back(program);

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
	vector<int> _column_args_style;
};

}

#endif  //  __DOWNPOUR__WORK_TABLE__H__

