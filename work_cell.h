#ifndef __DOWNPOUR__WORK_CELL__H__
#define __DOWNPOUR__WORK_CELL__H__

#include <string>

using namespace std;

namespace downpour {

class WorkCell {
public:
	enum state {
		EMPTY,
		ERROR,
		FINISHED
	};

	WorkCell() {
		_state = EMPTY;
	}

	virtual bool finished() const {
		return _state == FINISHED;
	}

	virtual string get() const {
		return _data;
	}

	virtual void set(const string& value) {
		if (_state == FINISHED && value != _data) {
			Logger::error("cell::set already full. % v %",
				      value, _data);
		}
		_data = value;
		_state = FINISHED;
	}

	virtual void error(const string& error) {
		_data = error;
		_state = ERROR;
	}

protected:
	state _state;
	string _data;
};

}

#endif  //  __DOWNPOUR__WORK_CELL__H__
