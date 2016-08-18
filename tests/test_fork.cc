#include <string>
#include <vector>
#include <unistd.h>

#include "ib/logger.h"
#include "downpour/mock_work_table.h"
#include "downpour/worker.h"

using namespace std;
using namespace downpour;
using namespace ib;

int main() {
	MockWorkTable table;
	table.set_mock(0, 0, "/bin/ls -l .", "");
	Worker worker(&table);

	worker.work();
}
