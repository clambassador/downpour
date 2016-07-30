# downpour

Downpour is a partial-result cacher for error-prone computations. Instead of
running a program, trying to deal with errors, saving results, etc., one write
their program as a set of smaller programs. These smaller programs as specified
by their command-line invocations. They can take previous results in as their
stdin; their stdout is their result. The data is storage as a table: rows
correspond to separate computations, columns are the steps in the computation.

The resulting program is specified by providing the command line invocation for
each step of the program, along with a suitable name for it and the list of
previous columns to pass alongside. A server manages all the cells and
determines which can be computed now. It provides to a worker client the program
and input to run.

To use downpour, simply create programs for the steps, and write the format
file. Then run the downpour server and worker. Your programs will run in the
correct order and the results will be saved. Return 0 if the result should be
considered correct, anything else for a failure.
