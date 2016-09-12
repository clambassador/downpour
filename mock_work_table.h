#ifndef __DOWNPOUR__MOCK_WORK_TABLE__H__
#define __DOWNPOUR__MOCK_WORK_TABLE__H__

#include <memory>
#include <string>
#include <vector>

#include "ib/logger.h"
#include "ib/marshalled.h"
#include "downpour/abstract_work_table.h"

using namespace std;

namespace downpour {

class MockWorkTable : public AbstractWorkTable {
public:
	MockWorkTable() : _cur(0) {}
	virtual ~MockWorkTable() {}

	virtual void get_work(size_t* row, size_t* col, string* what,
			      string* data) {
		if (_cur == _rows.size()) {
			*row = -1;
			*col = -1;
			*what = "";
			data->clear();
			return;
		}
		*row = _rows[_cur];
		*col = _cols[_cur];
		*what = _whats[_cur];
		*data = _datas[_cur];
		++_cur;
	}

	virtual void error(size_t row, size_t col, const string& result) {}
	virtual bool done_work(size_t row, size_t col, const string& result) {
		return true;
	}
	virtual void get_raw(vector<vector<string>>* out) const {}
	virtual void save() {}
	virtual void load() {}

	virtual string name() const { return ""; }

	virtual void set_mock(size_t row, size_t col, string what,
                              const string& data) {
		_rows.push_back(row);
		_cols.push_back(col);
		_whats.push_back(what);
		_datas.push_back(data);
	}

protected:
	vector<size_t> _rows, _cols;
	vector<string> _whats;
	vector<string> _datas;
	size_t _cur;
};

}

#endif  //  __DOWNPOUR__ABSTRACT_WORK_TABLE__H__
