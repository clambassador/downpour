#ifndef __DOWNPOUR__WORK_ROW__H__
#define __DOWNPOUR__WORK_ROW__H__

#include <memory>
#include <string>

#include "downpour/work_header.h"
#include "downpour/work_cell.h"

namespace downpour {

class WorkRow {
public:
	WorkRow(WorkHeader* header)
			: _work_cell(-1), _header(header) {
		for (size_t i = 0; i < _header->columns(); ++i) {
			_cells.push_back(unique_ptr<WorkCell>(nullptr));
			_cells.back().reset(new WorkCell());
		}
	}

	virtual ~WorkRow() {}

	string get(size_t col) const {
		assert(col < _cells.size());
		return _cells[col]->get();
	}

	void set(size_t col, const string& value) {
		assert(col < _cells.size());
		_cells[col]->set(value);
	}

	WorkCell* operator[](size_t col) {
		assert(col < _cells.size());
		return _cells[col].get();
	}

	size_t get_work() {
		while (true) {
			++_work_cell;
			if (_work_cell == _cells.size()) {
				_work_cell = -1;
				return -1;
			}
			assert(_work_cell < _cells.size());
			if (!_cells[_work_cell]->finished() &&
			    has_prereqs(_work_cell)) {
				return _work_cell;
			}
		}
	}

protected:
	virtual bool has_prereqs(size_t col) const {
		vector<size_t> args;
		_header->get_column_args(col, &args);
		for (const auto &x : args) {
			if (!_cells[x]->finished()) return false;
		}
		return true;
	}

	size_t _work_cell;

	WorkHeader* _header;
	vector<unique_ptr<WorkCell>> _cells;
};

}

#endif  //  __DOWNPOUR__WORK_TABLE__H__

