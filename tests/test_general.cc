#include "ib/logger.h"
#include "downpour/work_table.h"

#include <string>
#include <vector>
#include <unistd.h>

using namespace std;
using namespace downpour;
using namespace ib;

int main() {
	string format = "/tmp/format_test_general";
	string outfile = "/tmp/outfile_test_general";
	ofstream fout(format);
	unlink(outfile.c_str());

	fout << "FILES" << endl;
	fout << "ls /tmp" << endl;
	fout << "" << endl;
	fout << "SIZES" << endl;
	fout << "getsize" << endl;
	fout << "0" << endl;
	fout << "BLOCKS" << endl;
	fout << "blocks" << endl;
	fout << "1" << endl;

	fout.close();

	WorkTable table(format, outfile);
	table.initialize();

	bool fill = false;
	bool done = false;

	while (true) {
		size_t row = 0, col = 0;
		string what;
		string data;

		table.get_work("", 1, &row, &col, &what, &data);
		Logger::info("got work % % % %", row, col, what, data);
		if (!fill) {
			assert(row == -1);
			assert(col == 0);

			fill = true;
			table.done_work(-1, 0, "toot");
			table.done_work(-1, 0, "woop");
			table.done_work(-1, 0, "whee");
			table.done_work(-1, 0, "wham");
			table.done_work(-1, 0, "thee");
		} else if (col == 0) {
			if (done) {
				break;
			}
			done = true;
			table.done_work(-1, 0, "what");
			table.done_work(-1, 0, "topp");
		} else {
			assert(row != -1);
			static int j = 0;
			++j;
			if (j % 3) table.error(row, col, "bad news (but okay)");
			else table.done_work(row, col, Logger::stringify(
				"% % % answers", row, col, data[0]));
		}
	}

	vector<vector<string>> out;
	table.get_raw(&out);
	Logger::info("result %", out);

	table.save();
	WorkTable table_in(format, outfile);
	table_in.load();
	out.clear();
	table_in.get_raw(&out);
	Logger::info("save/load result %", out);
}
