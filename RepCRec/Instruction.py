import re
from enum import Enum


class InstructionType(Enum):
    """
    Enumeration of semantic instruction types used internally

    Members:
        READ  : A read request (R(...)).
        WRITE : A write request (W(...)).
    """
    READ      = 0
    WRITE     = 2


class InstructionIO:
    """
    InstructionIO is responsible for the I/O layer of the simulator.

    Attributes:
        file_name (str):
            Name of the input file. Can be None when stdin=True.
        stdin (bool):
            If True, input is read from standard input instead of a file.
        line_generator (generator | None):
            When stdin=False, this is a generator that yields non-empty lines from the input file.
        site_manager (SiteManager):
            Object that handles site-related instructions (dump, fail, recover).
        transaction_manager (TransactionManager):
            Object that handles transaction-related instructions.
    """

    # Operation strings
    OP_BEGIN        = "begin"
    OP_READ         = "R"
    OP_WRITE        = "W"
    OP_DUMP         = "dump"
    OP_END          = "end"
    OP_FAIL         = "fail"
    OP_RECOVER      = "recover"

    OP_SITE_MANAGER = [OP_DUMP, OP_FAIL, OP_RECOVER]

    PARAM_MATCHER = r"\((.*?)\)"

    class Instruction:
        """
        Represents a single parsed instruction such as "R(T1, x1)".

        Attributes:
            instruction_type: Operation string, e.g., "R", "W", "begin", "end", "fail".
            params: List of parameters inside the parentheses.
        """
        def __init__(self, raw: str):
            """
            Purpose:
                Construct an Instruction object from a raw instruction string.

            Args:
                raw (str): Raw instruction text
            """            
            self.instruction_type = raw.split('(')[0].strip()

            m = re.search(InstructionIO.PARAM_MATCHER, raw)
            if m:
                params_str = m.group(1)           # text inside parentheses
                parts = params_str.split(',')    

                self.params = [p.strip() for p in parts]
            else:
                self.params = []

        def get_params(self):
            """
            Purpose:
                Return the list of parameters for this instruction.

            Returns:
                List:The parameters parsed from the instruction, in order.
            """            
            return self.params

        def get_instruction_type(self):
            """
            Purpose: Return the operation string of this instruction.

            Returns: str: The instruction type.
            """            
            return self.instruction_type


    def __init__(self, file_name, site_manager, transaction_manager,
                  stdin=False):
        """
        Purpose:
            Initialize an InstructionIO instance that knows where to read instructions from and how to dispatch them.

        Args:
            file_name (str):
                Name of the input file to read from when stdin is False.
            site_manager (SiteManager):
                Object that should receive site-related instructions.
            transaction_manager (TransactionManager):
                Object that should receive transaction-related instructions.
            stdin (bool):
                If True, instructions are read from standard input using
                input(). If False, instructions are read from file_name.

        Side effects: If stdin is False, opens the file and creates a generator that will read lines from it when needed.
        """        
        
        self.file_name = file_name
        self.stdin = stdin
        if not self.stdin:
            self.line_generator = self._get_line_generator()

        self.site_manager = site_manager
        self.transaction_manager = transaction_manager

    def _get_line_generator(self):
        """
        Purpose:
            Create a generator that yields non-empty lines from the input file.

        Returns:
            generator: A generator which, on each iteration, yields the next non-empty line from the file.

        Side effects:
            Opens the file specified by self.file_name in read mode.
            Relies on the file system; if the file does not exist, an exception will be raised when this function is called.
        """
        with open(self.file_name, 'r') as input_file:
            for line in input_file:
                if len(line.strip()) > 0:
                    yield line

    def _process_instruction(self, line):
        """
        Purpose:
            Parse a single raw line into zero or more Instruction objects.

        Args:
            line: A raw input line which may contain multiple instructions separated by ';',
              and may also contain comments starting with "//".

        Returns:
            List: A list of Instruction objects created from this line, in the order they appear. 
        """
        pieces = line.strip().split(";")
        instructions = []
        for piece in pieces:
            piece = piece.strip()
            if not piece:
                continue
            if piece.startswith("//"):  
                continue
            instructions.append(self.Instruction(piece))
        return instructions

    def get_next_instruction(self):
        """
        Purpose:
            Read the next line of input (from file or stdin) and parse it

        Returns:
            List|None: If a new line is successfully read, returns the list of
                  Instruction objects parsed from that line; If there is no more 
                  input (end of file or EOF on stdin), returns None.

        Side effects:
            Consumes one line from the file generator or from stdin.
            May raise or internally catch EOFError when reading from stdin.
        """
        if not self.stdin:
            line = next(self.line_generator, None)
        else:
            try:
                line = input().strip()
            except EOFError:
                line = None

        if line is None:
            return None
        return self._process_instruction(line)

    def run(self):
        """
        Purpose:
            Main execution loop of the simulator input layer.
            Continuously fetches instructions and dispatches them to
            the appropriate manager until input is exhausted.

        Side effects:
            Repeatedly reads from the input source (file or stdin).
            For each parsed Instruction: If the operation type is in OP_SITE_MANAGER, calls self.site_manager.tick(inst).
                Otherwise, calls self.transaction_manager.tick(inst).
        """
        instructions = self.get_next_instruction()

        while instructions is not None:
            for inst in instructions:
                op = inst.get_instruction_type()
                if op in self.OP_SITE_MANAGER:
                    self.site_manager.tick(inst)
                else:
                    self.transaction_manager.tick(inst)

            instructions = self.get_next_instruction()
