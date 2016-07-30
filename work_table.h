#ifndef __DOWNPOUR__WORK_TABLE__H__
#define __DOWNPOUR__WORK_TABLE__H__

#include <memory>
#include <string>
#include <vector>

#include "downpour/logger.h"
#include "downpour/work_cell.h"
#include "downpour/work_header.h"
#include "downpour/work_row.h"

using namespace std;

namespace downpour {

class WorkTable {
public:
	WorkTable(const string& format, const string& storage)
		: _storage(storage), _format(format), _work_row(-1) {}
	virtual ~WorkTable() {
		save();
	}

	virtual void initialize() {
		assert(!_format.empty());
		assert(!_storage.empty());

		assert(!ifsteam(_storage).good());

		parse(_format);
	}

	virtual void load() {
		assert(!_format.empty());
		assert(!_storage.empty());

		ifstream fin(_storage);
		assert(fin.good());
		parse(_format);
		/* TODO : load in file data */

	}

	virtual void save() {
		// TODO: save
	}

/* TODO: mutex,
         rpc stub and service
	 http monitor
	 public keys for clients and sig checks incl. parameters and program
 */

	virtual void get_work(size_t* row, size_t* col, string* what,
			      vector<string>* data) {
		*col = find_work();
		if (*col == -1) {
			*col = 0;
			*row = -1;
			vector<size_t> args;
			_header->get_work(*col, what, &args);
			assert(!args.size());
			project(0, data);
			return;
		}
		*row = _work_row;

		WorkCell* cell = get_cell(*row, *col);
		assert(!cell->finished());
		vector<size_t> args;
		_header->get_work(*col, what, &args);
		assert(data);
		assert(!data->size());
		for (auto &x : args) {
			WorkCell* cell_arg = get_cell(*row, x);
			assert(cell_arg);
			assert(cell_arg->finished());
			data->push_back(cell_arg->get());
		}
	}

	virtual void error(size_t row, size_t col, const string& result) {
		assert(col > 0);
		WorkCell* cell = get_cell(row, col);
		assert(cell);
		cell->error(result);
	}

	virtual void done_work(size_t row, size_t col, string result) {
		if (col == 0) {
			assert(row == -1);
			add_row(result);
		} else {
			WorkCell* cell = get_cell(row, col);
			assert(cell);
			cell->set(result);
		}
	}

	virtual void get_raw(vector<vector<string>>* out) const {
		size_t cols = _header->columns();
		for (size_t row = 0; row < _rows.size(); ++row) {
			out->push_back(vector<string>());
			for (size_t col = 0; col < cols; ++col) {
				WorkCell const * cell = get_cell(row, col);
				if (!cell->finished()) {
					out->pop_back();
					break;
				}
				out->back().push_back(cell->get());
			}
		}
	}

protected:
	virtual WorkCell* get_cell(size_t row, size_t col) const {
		assert(row < _rows.size());
		assert(col < _header->columns());
		return (*_rows[row])[col];
	}

	virtual size_t find_work() {
		if (!_rows.size()) return -1;
		if (_work_row == -1) _work_row = 0;
		size_t start_row = _work_row;
		do {
			size_t col = _rows[_work_row]->get_work();
			if (col != -1) return col;
			++_work_row;
			if (_work_row == _rows.size()) {
				_work_row = 0;
			}
		} while (start_row != _work_row);
		return -1;
	}

	void add_row(const string& entry) {
		_rows.push_back(unique_ptr<WorkRow>(nullptr));
		_rows.back().reset(new WorkRow(_header.get()));
		_rows.back()->set(0, entry);
	}

	void project(size_t col, vector<string>* result) {
		for (auto &x : _rows) {
			result->push_back(x->get(col));
		}
	}

	void parse(const string& format) {
		_header.reset(new WorkHeader());
		_header->init(format);
	}

	unique_ptr<WorkHeader> _header;
	vector<unique_ptr<WorkRow>> _rows;

	string _storage;
	string _format;
	size_t _work_row;
};

}

#endif  //  __DOWNPOUR__WORK_TABLE__H__
