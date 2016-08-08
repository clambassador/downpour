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
	fout << "0" << endl;
	fout << "SIZES" << endl;
	fout << "getsize" << endl;
	fout << "1 0" << endl;
	fout << "BLOCKS" << endl;
	fout << "blocks" << endl;
	fout << "1 1" << endl;

	fout.close();

	WorkTable table(format, outfile);
	table.initialize();

	bool fill = false;

	while (true) {
		size_t row = 0, col = 0;
		string what;
		vector<string> data;

		table.get_work(&row, &col, &what, &data);

		Logger::info("got work % % % % %", row, col, what,
			     data.size(), data.size() ? data[0] : "nargs");
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
			Logger::info("out out data %", data);
			assert(data.size() >= 5);
			assert(data[4] == "thee");
			if (data.size() == 7) {
				assert(data[6] == "topp");
				break;
			}
			table.done_work(-1, 0, "what");
			table.done_work(-1, 0, "topp");
		} else {
			assert(row != -1);
			static int j = 0;
			++j;
			if (j % 3) table.error(row, col, "bad news");
			else table.done_work(row, col, Logger::stringify(
				"% % % answers", row, col, data[0]));
		}
	}

	vector<vector<string>> out;
	table.get_raw(&out);
	Logger::info("result %", out);
}
