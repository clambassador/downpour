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
	WorkHeader() {}

	virtual void init(const string& filename) {
		ifstream fin(filename);
		assert(fin.good());

		while (fin.good()) {
			string column_name, program;
			getline(fin, column_name);

			if (!fin.good()) break;
			getline(fin, program);
			size_t argc;
			fin >> argc;
			assert(argc <= _column_names.size());
			_column_args.push_back(vector<size_t>());
			for (size_t i = 0; i < argc; ++i) {
				size_t arg;
				set<size_t> test;
				fin >> arg;
				assert(arg <= _column_names.size());
				assert(!test.count(arg));
				test.insert(arg);

				_column_args.back().push_back(arg);
			}

			assert(column_name.size());
			assert(program.size());
			_column_names.push_back(column_name);
			_column_programs.push_back(program);

			getline(fin, column_name);
		}
		Logger::info("(downpour) Header: %", _column_names);
		Logger::info("(downpour) Programs: %", _column_programs);
	}

	virtual void get_work(size_t col,
			      string* what,
			      vector<size_t>* column_args) {
		assert(what);
		assert(column_args);

		*what = _column_programs[col];
		*column_args = _column_args[col];
	}

	virtual size_t columns() const {
		return _column_args.size();
	}

	virtual void get_column_args(size_t col, vector<size_t>* args) const {
		*args = _column_args[col];
	}

protected:
	vector<string> _column_names;
	vector<string> _column_programs;
	vector<vector<size_t>> _column_args;
};

}

#endif  //  __DOWNPOUR__WORK_TABLE__H__

