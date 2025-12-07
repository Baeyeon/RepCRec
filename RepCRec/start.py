import logging
import plac
from pathlib import Path

from .config import config
from .SiteManager import SiteManager
from .TransactionManager import TransactionManager
from .Instruction import InstructionIO


class Main:
    """
    Main is the entry point of the entire system.

    Args (CLI):
        file_path (str):
            Path to the input file that contains instructions in the
            project's language. When used with `stdin=True`, this can
            be any dummy value and is not read.
        num_sites (int, option -n):
            Number of database sites to create.
        num_variables (int, option -v):
            Number of logical variables (x1..xN) in the system.
        out_file (str, option -o):
            If provided, all logging output is written to this file.
            If omitted (None), logging goes to stdout.
        stdin (bool, flag -i):
            If True, instructions are read from standard input instead of
            from `file_path`.
    """

    @plac.annotations(
        file_path=("File name, pass anything in case of stdin",
                   "positional", None, str),
        num_sites=("Number of Sites", "option", "n", int),
        num_variables=("Number of variables", "option", "v", int),
        out_file=("Output file, if not passed we log to stdout",
                  "option", "o", str),
        stdin=("Take input from stdin instead of file if passed",
               "flag", "i"))
    def __init__(self, file_path,
                 num_sites=config['NUM_SITES'],
                 num_variables=config['NUM_VARIABLES'],
                 out_file=None,
                 stdin=False):
        """
        Constructor. Sets up logging, creates SiteManager, TransactionManager,
        and InstructionIO.

        Args:
            file_path (str):
                Path to the instruction file (ignored when stdin=True).
            num_sites (int):
                Number of sites to create.
            num_variables (int):
                Number of logical variables in the system.
            out_file (str | None):
                If not None, path to a log file to write all logs into.
            stdin (bool):
                If True, instructions are read from stdin; otherwise from
                the file at `file_path`.
        Returns:
            None.
        Side effects:
            - Configures global logging.
            - Instantiates SiteManager, TransactionManager, and InstructionIO.
        """
        p = Path('.') / file_path

        # Prepare log file if requested
        if out_file:
            open(out_file, 'w').close()

        logging.basicConfig(
            filename=out_file,
            format='%(levelname)s - %(asctime)s - %(message)s',
            level=config['LOG_LEVEL']
        )

        # Core components
        self.site_manager = SiteManager(num_sites, num_variables)
        self.transaction_manager = TransactionManager(
            num_variables, num_sites, self.site_manager
        )

        # Allow SiteManager to notify TransactionManager on failures
        self.site_manager.txn_manager = self.transaction_manager

        # Driver that reads instructions and dispatches to managers
        self.io = InstructionIO(
            p,
            self.site_manager,
            self.transaction_manager,
            stdin=stdin
        )

    def run(self):
        """
        Run the instruction-processing loop.

        Returns:
            None.
        Side effects:
            - Reads instructions either from file or stdin.
            - For each instruction, calls TransactionManager.tick(...) or
              SiteManager.tick(...).
            - Drives the entire lifetime of the test workload.
        """
        self.io.run()


if __name__ == "__main__":
    # Parse command-line arguments and construct the Main object
    main = plac.call(Main)
    main.run()
