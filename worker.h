#ifndef __DOWNPOUR__WORKER__H__
#define __DOWNPOUR__WORKER__H__

#include <cstring>
#include <string>
#include <unistd.h>

#include "abstract_work_table.h"

using namespace std;

namespace downpour {

class Worker {
public:
	Worker(AbstractWorkTable* work_table) : _work_table(work_table) {}

	virtual void work() {
		while (true) {
			size_t row;
			size_t col;
			string what;
			vector<string> data;
			_work_table->get_work(&row, &col, &what, &data);
			if (row == -1) {
				// TODO: do the create more work
				break;
			}

			string result;
			int error = do_work(row, col, what, data, &result);
			Logger::info("result % %", error, result);
		}
	}

	virtual int do_work(size_t row, size_t col, const string& what,
			    const vector<string>& data, string* result) {
		Logger::info("doing work for % %: %", row, col, what);
		pid_t pid = 0;
		int pipefd[2];
		unique_ptr<char[]> buf(new char[1<<20]);

		size_t pos = 0;
		vector<string> args;
		while (true) {
			size_t start = pos;
			pos = what.find(" ", start);
			if (pos == string::npos) {
				args.push_back(what.substr(start));
				break;
			}
			args.push_back(what.substr(start, pos -
						   start));
			++pos;
		}
		Logger::info("args %", args);
		pipe(pipefd);
		pid = fork();
		if (pid == 0) {
			// forked child
			dup2(pipefd[0], STDIN_FILENO);
			dup2(pipefd[1], STDOUT_FILENO);
			dup2(pipefd[1], STDERR_FILENO);
			char** argv = (char**) malloc(args.size() * sizeof(char*));
			for (int i = 0; i < args.size(); ++i) {
				argv[i] = (char*) args[i].c_str();
			}
	//		execv(argv[0], &argv[1]);
			execl("ls", "/bin/ls");
			free(argv);
			exit(1);
		}
		for (auto &x : data) {
			string s = Marshalled(x).str();
			write(pipefd[1], s.c_str(), s.length());
		}
		memset(buf.get(), 0, 1 << 20);
		int r = read(pipefd[0], buf.get(), 1 << 20);
		Logger::info("r % %", r, buf.get());
		return 0;
	}

protected:
	AbstractWorkTable* _work_table;
};

}  // namespace downpour

#endif  // __DOWNPOUR__WORKER__H__
