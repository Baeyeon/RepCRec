import logging
from collections import defaultdict

from .Transaction import Transaction

from .Transaction import TransactionStatus
from .Instruction import InstructionIO
from .Site import SiteStatus

log = logging.getLogger(__name__)


class TransactionManager:
    """
    Coordinates all transactions and enforces SSI.

    Attributes:
        number_of_variables (int): Total number of logical variables.
        number_of_sites (int): Number of replication sites.
        site_manager (SiteManager): Used to read/write values on sites.
        transaction_map (dict[str, Transaction]): Active transactions by name.
        current_time (int): Logical time used for timestamps.

        last_commit_ts (dict[str, int]): Last commit time per variable.
        last_writer (dict[str, str]): Last committing transaction per variable.
        version_history (dict[str, list[(int, str)]]): (commit_ts, txn_name)
            history per variable, for version reasoning.
        dep_graph (dict[str, set[str]]): SSI dependency graph edges.
    """

    def __init__(self, num_vars, num_sites, site_manager):
        """
        Initialize a TransactionManager instance.

        Args:
            num_vars (int): Total number of logical variables.
            num_sites (int): Number of replication sites.
            site_manager (SiteManager): Manager used to access sites.

        Side effects:
            - Sets up empty metadata structures (transaction_map, timestamps,
              SSI graph) for later use.
        """        
        self.number_of_variables = num_vars
        self.number_of_sites = num_sites
        self.transaction_map = dict()
        self.site_manager = site_manager
        self.current_time = 0

        self.last_commit_ts = {}   
        self.last_writer = {}     

        self.version_history = defaultdict(list)
        self.dep_graph = defaultdict(set)

    def tick(self, instruction):
        """
        Dispatch a single parsed instruction to the correct handler.

        Args:
            instruction (InstructionIO.Instruction): Parsed instruction from IO.

        Side effects:
            - Advances logical time.
            - May create, read, write, or commit/abort transactions.
        """
        self.current_time += 1
        self.clear_aborted()

        params = list(instruction.get_params())

        if instruction.get_instruction_type() == InstructionIO.OP_BEGIN:
            self.begin(params)

        elif instruction.get_instruction_type() == InstructionIO.OP_READ:
            self.read_request(params)

        elif instruction.get_instruction_type() == InstructionIO.OP_WRITE:
            self.write_request(params)

        elif instruction.get_instruction_type() == InstructionIO.OP_END:
            self.end(params)
        else:
            log.info("Here is a problem")


    def begin(self, params):
        """
        Start a new transaction and take its initial snapshot.

        Args:
            params (list[str]): [transaction_name].

        Side effects:
            - Creates a Transaction object.
            - Sets start_ts and snapshot for that transaction.
            - Stores it in transaction_map.
        """        
        name = params[0]
        current_index = len(self.transaction_map)

        txn = Transaction(current_index, name)
        txn.start_ts = self.current_time

        txn.snapshot = self.site_manager.get_current_variables()

        self.transaction_map[name] = txn
        log.info(f"Starting transaction {name} at ts={txn.start_ts}")


    def write_request(self, params):
        """
        Handle a write request under Snapshot Isolation.

        Args:
            params (list[str]): [txn_name, variable_name, value_str].

        Side effects:
            - Buffers the write in txn.uncommitted_variables.
            - Records the variable in txn.write_set.
            - Records which sites were up when this variable was written, for Available-Copies handling on commit.
        """
        txn_name = params[0]
        var = params[1]
        value = int(params[2])

        if txn_name not in self.transaction_map:
            return

        txn = self.transaction_map[txn_name]
        status = txn.get_status()

        if status in (TransactionStatus.ABORTED,
                      TransactionStatus.COMMITTED):
            return

        txn.uncommitted_variables[var] = value

        log.info(f"{txn_name} buffered write {var} = {value}")

        txn.write_set.add(var)

        from .Variable import Variable
        from .SiteManager import SiteStatus   

        var_index = int(var[1:])               
        sites = Variable.get_sites(var_index)     
        sites = self.site_manager.get_site_range(sites)

        for site_id in sites:
            site_obj = self.site_manager.get_site(site_id)
            if site_obj.get_status() != SiteStatus.DOWN:
                txn.write_sites.add(site_id)

    def abort_transactions_on_site_failure(self, site_id: int):
        """
        Abort all transactions that have written to a failed site.

        Args:
            site_id (int): Id of the site that just failed.

        Side effects:
            - Marks any such transaction as ABORTED.
        """
        for txn in self.transaction_map.values():
            status = txn.get_status()
            if status in (TransactionStatus.ABORTED, TransactionStatus.COMMITTED):
                continue

            if site_id in txn.write_sites:
                log.info(f"{txn.name} aborted as site {site_id} failed")
                txn.set_status(TransactionStatus.ABORTED)

    def read_request(self, params):
        """
        Handle a read request under Snapshot Isolation.

        Args:
            params (list[str]): [txn_name, variable_name].

        Side effects:
            - May mark the transaction as ABORTED if the variable
              is not in its snapshot.
            - Records the read in txn.read_set / txn.read_variables.
        """
        txn_name = params[0]
        var = params[1]

        if txn_name not in self.transaction_map:
            return

        txn = self.transaction_map[txn_name]
        status = txn.get_status()

        if status in (TransactionStatus.ABORTED,
                      TransactionStatus.COMMITTED):
            return

        if var in txn.uncommitted_variables:
            val = txn.uncommitted_variables[var]
        else:
            if var not in txn.snapshot:
                log.info(
                    f"{txn_name} cannot read {var} because it is not in the snapshot; aborting {txn_name}"
                )
                txn.set_status(TransactionStatus.ABORTED)
                return

            val = txn.snapshot[var]

        txn.read_set.add(var)
        if var not in txn.read_variables:
            txn.read_variables[var] = []
        txn.read_variables[var].append(val)

        log.info(f"{txn_name} read the value {val} of variable {var}")



    def _get_version_writer(self, var, snapshot_ts):
        """
        Find the last writer of a variable before a given time.

        Args:
            var (str): Variable name (e.g., "x3").
            snapshot_ts (int): Snapshot time to search up to.

        Returns:
            str | None: Name of the writing transaction, or None if
            no committed version exists before snapshot_ts.
        """
        history = self.version_history.get(var, [])
        writer = None
        for commit_ts, name in history:
            if commit_ts <= snapshot_ts:
                writer = name
            else:
                break
        return writer

    def _add_edge(self, frm, to):
        """
        Add a directed edge to the SSI dependency graph.

        Args:
            frm (str): Source transaction name.
            to (str): Destination transaction name.

        Side effects:
            - Updates dep_graph[frm] to include 'to'.
        """
        if frm == to:
            return
        self.dep_graph[frm].add(to)

    def _has_cycle_from(self, start_name):
        """
        Check if there is a cycle in the SSI graph reachable from a node.

        Args:
            start_name (str): Name of the transaction to start DFS from.

        Returns:
            bool: True if a cycle is found, False otherwise.
        """
        visited = set()
        stack = set()

        def dfs(u):
            visited.add(u)
            stack.add(u)
            for v in self.dep_graph.get(u, []):
                if v not in visited:
                    if dfs(v):
                        return True
                elif v in stack:
                    return True
            stack.remove(u)
            return False

        return dfs(start_name)

    def _remove_txn_from_graph(self, name):
        """
        Remove a transaction from the SSI dependency graph.

        Args:
            name (str): Transaction name to remove.

        Side effects:
            - Deletes 'name' node and removes it from all adjacency sets.
        """
        if name in self.dep_graph:
            self.dep_graph.pop(name)

        for nbrs in self.dep_graph.values():
            nbrs.discard(name)

    def _record_conflicts_on_commit(self, txn):
        """
        Add SSI graph edges based on this transaction's writes.

        Args:
            txn (Transaction): Transaction that is about to commit.

        Side effects:
            - Updates dep_graph with: rw edges: reader -> writer for read-old / write-new
                                    ww edges: earlier-writer -> later-writer, unless the
                                    same pair already has an rw edge on that variable.
        """
        name = txn.name

        for var in txn.write_set:
            for other_name, other in self.transaction_map.items():
                if other_name == name:
                    continue
                if other.get_status() == TransactionStatus.ABORTED:
                    continue

                other_reads = getattr(other, "read_set", set())
                other_writes = getattr(other, "write_set", set())

                has_read = var in other_reads
                has_write = var in other_writes

                if has_read:
                    if other.start_ts is not None and txn.commit_ts is not None \
                            and other.start_ts < txn.commit_ts:
                        self._add_edge(other_name, name)

                if has_write and not has_read:
                    if (other.start_ts is not None and txn.start_ts is not None
                            and other.start_ts <= txn.start_ts):
                        self._add_edge(other_name, name)
                    else:
                        self._add_edge(name, other_name)

    def clear_aborted(self):
        """
        Clean up transactions that are already marked ABORTED.

        Side effects:
            - Calls abort(name) for each aborted transaction, which
              removes it from the SSI graph.
        """
        to_pop = list()

        for trn_name in list(self.transaction_map):
            transaction = self.transaction_map[trn_name]
            if transaction.get_status() == TransactionStatus.ABORTED:

                to_pop.append(trn_name)
                self.abort(trn_name)
    
    def abort(self, name):
        """
        Mark a transaction as aborted and remove it from SSI graph.

        Args:
            name (str): Name of the transaction to abort.

        Side effects:
            - Sets txn.status to ABORTED if the transaction exists.
            - Removes it from dep_graph.
        """
        if name not in self.transaction_map:
            return

        txn = self.transaction_map[name]
        txn.set_status(TransactionStatus.ABORTED)

        # Remove from SSI dependency graph (safe even if not present).
        self._remove_txn_from_graph(name)    

    def commit_transaction(self, name):
        """
        Attempt to commit a transaction under SI + SSI.

        Args:
            name (str): Name of the transaction to commit.

        Side effects:
            - May mark the transaction as ABORTED (SI write-write conflict
              or SSI cycle).
            - On success, writes its buffered values to the appropriate
              sites and updates SI/SSI metadata.
        """
        if name not in self.transaction_map:
            return

        txn = self.transaction_map[name]
        status = txn.get_status()

        if status in (TransactionStatus.COMMITTED,
                      TransactionStatus.ABORTED):
            return

        for var in txn.uncommitted_variables.keys():
            last_ts = self.last_commit_ts.get(var, -1)
            last_writer = self.last_writer.get(var, None)

            if last_ts > txn.start_ts and last_writer != name:
                log.info(
                    f"{name} aborted due to SI write-write conflict on {var}: "
                    f"last writer {last_writer} at ts={last_ts}, "
                    f"txn.start_ts={txn.start_ts}"
                )
                txn.set_status(TransactionStatus.ABORTED)
                return

        self.current_time += 1
        txn.commit_ts = self.current_time

        for var in txn.uncommitted_variables.keys():
            self.version_history[var].append((txn.commit_ts, name))

        self._record_conflicts_on_commit(txn)

        if self._has_cycle_from(name):
            txn.set_status(TransactionStatus.ABORTED)
            txn.clear_uncommitted_variables()
            self._remove_txn_from_graph(name)
            return

        from .Variable import Variable  

        for var, value in txn.uncommitted_variables.items():
            idx = int(var[1:])  

            sites = Variable.get_sites(idx)
            sites = self.site_manager.get_site_range(sites)

            for site_id in sites:
                if site_id not in txn.write_sites:
                    continue

                site = self.site_manager.get_site(site_id)

                if site.get_status() == SiteStatus.DOWN:
                    continue

                site.write_variable(txn, var, value)

            self.last_commit_ts[var] = self.current_time
            self.last_writer[var] = name

        txn.set_status(TransactionStatus.COMMITTED)

    def end(self, params):
        """
        Handle an end(Ti) instruction: try to commit the transaction.

        Args:
            params (list[str]): [transaction_name].

        Side effects:
            - Calls commit_transaction(name).
            - Logs whether the transaction committed or aborted.
            - Removes aborted transactions from the SSI graph.
        """
        name = params[0]

        if name not in self.transaction_map:
            return

        status = self.transaction_map[name].get_status()
        if status in (TransactionStatus.COMMITTED,
                      TransactionStatus.ABORTED):
            return

        self.commit_transaction(name)

        final_status = self.transaction_map[name].get_status()
        if final_status == TransactionStatus.COMMITTED:
            log.info(f"{name} committed")

        elif final_status == TransactionStatus.ABORTED:
            log.info(f"{name} aborted at end")
            self._remove_txn_from_graph(name)
