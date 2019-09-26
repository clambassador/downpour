#ifndef __DOWNPOUR__WORK_TABLE__H__
#define __DOWNPOUR__WORK_TABLE__H__

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "ib/formatting.h"
#include "ib/logger.h"
#include "ib/tokenizer.h"
#include "ib/sensible_time.h"
#include "ib/marshalled.h"
#include "downpour/abstract_work_table.h"
#include "downpour/work_cell.h"
#include "downpour/work_header.h"
#include "downpour/work_row.h"

using namespace std;

namespace downpour {

class WorkTable : public AbstractWorkTable {
public:
	WorkTable(const string& format, const string& storage)
		: WorkTable(format, storage, vector<string>()) {}
	WorkTable(const string& format,
		  const string& storage,
		  const vector<string>& params)
		: _storage(storage), _format(format), _work_row(-1),
		_last_save(0), _loops(0), _params(params),
		_mutex(new mutex()), _built(false), _num_workers(1) {}

	virtual ~WorkTable() {
		save();
		output_csv();
	}

	virtual void initialize() {
		unique_lock<mutex> lock(*_mutex.get());
		initialize_impl();
	}

	virtual void initialize_impl() {
		assert(!_format.empty());
		assert(!_storage.empty());

		assert(!ifstream(_storage).good());

		parse(_format);
	}

	virtual void load() {
		unique_lock<mutex> lock(*_mutex.get());
		assert(!_format.empty());
		assert(!_storage.empty());

		ifstream fin(_storage);
		if (!fin.good()) {
			Logger::info("(downpour) No file % to load. Calling init()",
				     _storage);
			initialize_impl();
			return;
		}
		parse(_format);
		stringstream ss;
		ss << fin.rdbuf();
		Marshalled ml;
		ml.data(ss.str());
		size_t rows, cols;
		ml.pull(&rows, &cols);

		Logger::info("(downpour) File has % cols, header has %",
			     cols, _header->columns());
		assert(cols == _header->columns());
		/* Actually, adding cols will be allowed. For now just assert
		 * and later do a mechanism to protect against accidents. */

		for (size_t i = 0; i < rows; ++i) {
			add_row("");
			assert(get_cell(i, 0)->get().empty());
			for (size_t j = 0; j < cols; ++j) {
				ml.pull(get_cell(i, j));
			}
			assert(!get_cell(i, 0)->get().empty());
			_row_names.insert(get_cell(i, 0)->get());
		}

	}

	virtual string name() const {
		return _storage;
	}

	virtual void save() {
		unique_lock<mutex> lock(*_mutex.get());
		save_impl();
	}
	virtual void save_impl() {
		Logger::info("(downpour) Saving %x% to %",
			     _rows.size(), _header->columns(),
			     _storage);
		Marshalled ml(_rows.size(), _header->columns());
		for (size_t i = 0; i < _rows.size(); ++i) {
			for (size_t j = 0; j < _header->columns(); ++j) {
				ml.push(*get_cell(i, j));
			}
		}
		ofstream fout(_storage+ ".tmp");
		fout << ml.str();
		fout.close();
		rename((_storage +".tmp").c_str(), _storage.c_str());
		Logger::info("(downpour) saved successfully.");
	}

/* TODO:
         rpc stub and service
	 http monitor
	 public keys for clients and sig checks incl. parameters and program
	 list of allowed routines to be run, only do if on list
 */

	virtual void get_work(const string& name, size_t number,
			      size_t* row, size_t* col, string* what,
			      string* data) {
		unique_lock<mutex> lock(*_mutex.get());
		if (_num_workers < number) _num_workers = number;
		return get_work_impl(row, col, what, data);
	}

	virtual void get_work_impl(size_t* row, size_t* col, string* what,
			      string* data) {
		*row = -1;
		*col = find_work();
		if (*col == -1) {
			if (leased(-1, 0) || _built) {
				*what = "";
				*data = "";
				return;
			}
			_built = true;
			lease(-1, 0);
			*col = 0;
			*row = -1;
			vector<size_t> args;
			int style;
			_header->get_work(*col, what, &args, &style);
			assert(!args.size());
			*data = "";
			return;
		}
		*row = _work_row;

		WorkCell* cell = get_cell(*row, *col);
		assert(!cell->finished());
		vector<size_t> args;
		int argstyle;
		_header->get_work(*col, what, &args, &argstyle);
		stringstream ss;
		size_t argcol = -1;
		string pre, post;
		while (Tokenizer::extract("%$(%)%", *what, &pre, &argcol, &post)) {
			if (argcol == 0) {
				ss << pre << *row;
			} else {
				assert(argcol > 0 && argcol <= _header->columns());
				if (!get_cell(*row, argcol - 1)->finished()) {
					release(*row, *col);
					return get_work_impl(row, col, what, data);
				}
				ss << pre << get_cell(*row, argcol - 1)->get();
			}
			*what = post;
		}
		ss << *what;
		*what = ss.str();

		assert(data);
		assert(!data->size());
		vector<string> arg_data;
		for (auto &x : args) {
			WorkCell* cell_arg = get_cell(*row, x);
			assert(cell_arg);
			assert(cell_arg->finished());
			arg_data.push_back(cell_arg->get());
		}
		if (argstyle == WorkHeader::RAW) {
			for (auto &x : arg_data) {
				*data += x;
			}
		}
		if (argstyle == WorkHeader::NEWLINE) {
			for (auto &x : arg_data) {
				*data += x + '\n';
			}
		}
		if (argstyle == WorkHeader::STRING) {
			Marshalled ml;
			for (auto &x : arg_data) {
				ml.push(x);
			}
			*data = ml.str();
		}
		if (argstyle == WorkHeader::VECTOR) {
			*data = Marshalled(arg_data).str();
		}
	}

	virtual void error(size_t row, size_t col, const string& result) {
		unique_lock<mutex> lock(*_mutex.get());
		assert(col > 0);
		WorkCell* cell = get_cell(row, col);
		assert(cell);
		cell->error(result);
		release(row, col);
	}

	virtual bool done_work(size_t row, size_t col, const string& result) {
		unique_lock<mutex> lock(*_mutex.get());
		bool retval = true;
		if (col == 0) {
			assert(row == -1);
			assert(!result.empty());
			retval = add_row(Tokenizer::trim(result));
		} else {
			WorkCell* cell = get_cell(row, col);
			assert(cell);
			cell->set(Tokenizer::trim(result));
		}
		maybe_save();
		release(row, col);
		return retval;
	}

/*	virtual void iterate(const F_TableCell& cb) {
		size_t cols = _header->columns();
		for (size_t row = 0; row < _rows.size(); ++row) {
			for (size_t col = 0; col < cols; ++col) {
				cb(get_cell(row, col));
			}
		}
	}
*/

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

	virtual void output_csv() const {
		string filename = _storage + ".csv";
		ofstream fout(filename);

		size_t cols = _header->columns();
		for (size_t row = 0; row < _rows.size(); ++row) {
			for (size_t col = 0; col < cols; ++col) {
				WorkCell const * cell = get_cell(row, col);
				if (cell)
					fout << Formatting::csv_escape(cell->get());
				if (col < cols - 1) fout << ',';
			}
			fout << endl;
		}
		fout.close();
	}

	virtual void trace() const {
		stringstream ss;
		size_t cols = _header->columns();
		vector<size_t> widths;
		widths.resize(cols);
		for (size_t row = 0; row < _rows.size(); ++row) {
			for (size_t col = 0; col < cols; ++col) {
				WorkCell const * cell = get_cell(row, col);
				if (cell->get().length() > widths[col]) {
					widths[col] = cell->get().length();
				}
				if (row == _rows.size() - 1) {
					if (col == 0) ss << "        ";
					string s =
					    _header->get_column_name(col);
					s.resize(widths[col], ' ');
					ss << s << "   ";
				}
			}
		}
		ss << endl;
		for (size_t row = 0; row < _rows.size(); ++row) {
			ss << "[" << row << "] \t";
			for (size_t col = 0; col < cols; ++col) {
				WorkCell const * cell = get_cell(row, col);
				string value = cell->get();
				for (int i = 0; i < value.length(); ++i) {
					if (value[i] == '\n') value[i] = ' ';
				}
				value.resize(widths[col], ' ');
				ss << value << "   ";
			}
			ss << endl;
		}
		Logger::info("(downpour) table data:\n\n%\n\n", ss.str());
	}

	virtual bool exhausted() const {
		return _loops > _num_workers * 3;
	}

protected:
	virtual bool leased(size_t row, size_t col) {
		return _leased[row][col];
	}

	virtual void lease(size_t row, size_t col) {
		assert(!leased(row, col));
		_leased[row][col] = true;
	}

	virtual void release(size_t row, size_t col) {
		if (row != -1) assert(leased(row, col));
		_leased[row][col] = false;
	}

	virtual void maybe_save() {
		if (sensible_time::runtime() - _last_save > 10) {
			save_impl();
			_last_save = sensible_time::runtime();
		}
	}

	virtual WorkCell* get_cell(size_t row, size_t col) const {
		assert(row < _rows.size());
		assert(col < _header->columns());
		return (*_rows[row])[col];
	}

	virtual size_t find_work() {
		if (!_rows.size()) return -1;
		if (_work_row == -1) {
			_work_row = 0;
		}
		size_t start_row = _work_row;
		do {

			size_t col = _rows[_work_row]->get_work();
			if (col != -1) {
				if (!leased(_work_row, col)) {
					lease(_work_row, col);
					return col;
				}
			}
			++_work_row;
			if (_work_row == _rows.size()) {
				_work_row = 0;
				_loops++;
			}
		} while (start_row != _work_row);
		return -1;
	}

	bool add_row(const string& entry) {
		if (!entry.empty() && _row_names.count(entry)) return false;
		_rows.push_back(unique_ptr<WorkRow>(nullptr));
		_rows.back().reset(new WorkRow(_header.get()));
		_rows.back()->set(0, entry);
		_row_names.insert(entry);
		return true;
	}

	void project(size_t col, vector<string>* result) {
		for (auto &x : _rows) {
			result->push_back(x->get(col));
		}
	}

	void parse(const string& format) {
		_header.reset(new WorkHeader());
		_header->init(format, _params);
	}

	unique_ptr<WorkHeader> _header;
	vector<unique_ptr<WorkRow>> _rows;

	string _storage;
	string _format;
	size_t _work_row;
	int _last_save;
	set<string> _row_names;
	size_t _loops;
	vector<string> _params;
	unique_ptr<mutex> _mutex;
	bool _built;
	map<size_t, map<size_t, bool>> _leased;
	size_t _num_workers;
};

}

#endif  //  __DOWNPOUR__WORK_TABLE__H__
