#include "downpour/logger.h"

namespace downpour {

unique_ptr<mutex> Logger::_mutex(new mutex());
function<void()> Logger::_fn(bind(&exit,0));
int Logger::_cur_fd = 0;
unordered_map<int, FILE *> Logger::_fd_to_file;

}  // namespace downpour
