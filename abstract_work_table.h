#ifndef __DOWNPOUR__ABSTRACT_WORK_TABLE__H__
#define __DOWNPOUR__ABSTRACT_WORK_TABLE__H__

#include <memory>
#include <string>
#include <vector>

#include "ib/logger.h"
#include "ib/marshalled.h"
#include "downpour/work_cell.h"
#include "downpour/work_header.h"
#include "downpour/work_row.h"

using namespace std;

namespace downpour {

class AbstractWorkTable {
public:
	virtual ~AbstractWorkTable() {}
	virtual void get_work(size_t* row, size_t* col, string* what,
			      string* data) = 0;
	virtual void error(size_t row, size_t col, const string& result) = 0;
	virtual bool done_work(size_t row, size_t col, const string& result) = 0;
	virtual void get_raw(vector<vector<string>>* out) const = 0;
	virtual void save() = 0;
	virtual void load() = 0;
	virtual string name() const = 0;
	virtual bool exhausted() const = 0;
};

}

#endif  //  __DOWNPOUR__ABSTRACT_WORK_TABLE__H__
