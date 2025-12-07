import logging
from .Variable import Variable
from enum import Enum

log = logging.getLogger(__name__)

class SiteStatus(Enum):
    """
    Enum describing the status of a single site.

    Values:
        UP:         Site is fully up and available for reads and writes.
        DOWN:       Site has failed and is unavailable for any access.
        RECOVERING: Site has recovered from failure but replicated (even)
                    variables may not yet be readable until a committed
                    write happens.
    """
    UP = 0
    DOWN = 1
    RECOVERING = 2

class Site:
    """
    Represents a single site in the replicated concurrency-control system.

    Attributes:
        id (int): Identifier of this site (1..num_sites).
        status (SiteStatus): Current status of the site.
        last_failure_time (int | None): Logical time of the last failure,
            if tracked by the upper layers.
        data_manager (Site.DataManager): Inner object that stores variables.
        recovered_variables (set[str]): Names of variables that are currently
            safe to read at this site under the Available Copies rules.
    """

    class DataManager:
        """
        Stores and manages all variables that physically live on a given site.

        Attributes:
            site_id (int): Identifier of the owning site.
            variable_map (dict[str, Variable]): Mapping from variable name
                (e.g., "x3") to Variable instances stored at this site.
        """

        def __init__(self, site_id):
            """
            Initialize the DataManager for a given site.

            Args:
                site_id (int):
                    Identifier of the site that owns this DataManager.

            Side Effects:
                - Populates `variable_map` with Variable objects for all
                  variables that should live on this site according to the
                  replication rule:
                    * all even-index variables are stored at every site;
                    * odd-index variables x_i are stored only at site (1 + i % 10).
            """            
            self.site_id = site_id
            self.variable_map = dict()

            for i in range(1, 21):
                if i % 2 == 0 or (1 + i % 10) == site_id:
                    variable = Variable(i, 'x' + str(i), 10 * i, site_id)
                    self.variable_map['x' + str(i)] = variable

        def add_variable(self, name, variable):
            """
            Add a variable to this site's local storage.

            Args:
                name (str): Name of the variable, e.g., "x3".
                variable (Variable): Variable instance to store.

            Side Effects:
                - Inserts or overwrites the entry in `variable_map` under
                  the given name.
            """
            self.variable_map[name] = variable

        def get_variable(self, name):
            """
            Look up a variable by name on this site.

            Args:
                name (str): Name of the variable to retrieve.

            Returns:
                Variable | None:
                    The Variable instance if present; otherwise None.
            """            
            if name in self.variable_map:
                return self.variable_map[name]
            else:
                return None

        def has_variable(self, name):
            """
            Check whether this site stores a variable with the given name.

            Args:
                name (str): Name of the variable to check, e.g., "x5".

            Returns:
                bool:
                    True if the variable is stored in `variable_map`,
                    False otherwise.
            """            
            return name in self.variable_map


        def write_variable(self, transaction, variable_name, value):
            """
            Apply a write to a variable in this site's local storage.

            Args:
                transaction (Transaction):
                    Transaction issuing the write. Only used for logging or
                    higher-level bookkeeping; not used directly here.
                variable_name (str):
                    Name of the variable to write, e.g., "x8".
                value (int):
                    New value to store.

            Returns:
                bool:
                    True if the variable exists on this site and the write
                    was applied; False if this site does not store the
                    variable at all.

            Side Effects:
                - Mutates the underlying Variable's value in `variable_map`
                  if the variable exists.
            """
            if variable_name in self.variable_map:
                self.variable_map[variable_name].set_value(value)
                return True
            else:
                return False

        def get_variables(self):
            """
            Get the mapping of all variables stored at this site.

            Returns:
                dict[str, Variable]:
                    Dictionary mapping variable names to Variable instances.
            """            
            return self.variable_map

    def __init__(self, index):
        """
        Construct a Site object with the given site index.

        Args:
            index (int):
                Identifier of the site (1..num_sites).

        Side Effects:
            - Initializes the site's DataManager with the correct variables.
            - Populates `recovered_variables` with:
                * all even variables stored at this site, and
                * the odd variable that belongs exclusively to this site
                  according to the schema.
        """
        self.id = index

        # Variables are mainly in DataManager, here only for convenience
        self.variables = []
        self.status = SiteStatus.UP
        self.last_failure_time = None
        self.data_manager = Site.DataManager(self.id)
        self.recovered_variables = set()

        for i in range(1, 21):

            if i % 2 == 0 or (1 + i % 10) == self.id:
                self.recovered_variables.add('x' + str(i))

    def set_status(self, status):
        """
        Update the status of this site.

        Args:
            status (SiteStatus):
                New status to assign to this site (UP, DOWN, or RECOVERING).

        Side Effects:
            - Mutates `self.status`.
            - Logs an error if the given status is invalid.
        """

        if status in SiteStatus:
            self.status = status
        else:
            log.error("Invalid Site status")
        return

    def get_status(self):
        """
        Get the current status of this site.

        Returns:
            SiteStatus:
                The current status (UP, DOWN, or RECOVERING).
        """
        return self.status

    def get_id(self):
        """
        Get the identifier of this site.

        Returns:
            int:
                Site identifier (1..num_sites).
        """
        return self.id

    def get_last_failure_time(self):
        """
        Get the last recorded failure time of this site.

        Returns:
            int | None:
                Logical time (if tracked) of the last failure, or None
                if no failure time has been recorded.
        """
        return self.last_failure_time

    def set_last_failure_time(self, time):
        """
        Record the logical time when this site last failed.

        Args:
            time (int):
                Logical timestamp representing the failure time.

        Side Effects:
             Mutates `self.last_failure_time`.
        """
        self.last_failure_time = time

    

    def write_variable(self, transaction, variable, value):
        """
        Apply a committed write to a variable at this site under
        Snapshot Isolation and Available Copies semantics.

        Args:
            transaction (Transaction):
                Transaction committing the write. Provided for consistency
                and possible logging; not directly used in this method.
            variable (str):
                Variable name to write, e.g., "x4".
            value (int):
                Value to store for the variable.

        Returns:
            bool:
                True if the write was accepted and applied at this site;
                False if the site is DOWN and the write is ignored.

        Side Effects:
            - May mutate the underlying variable's value in the DataManager.
            - If the site is RECOVERING and the variable is an even index
              (replicated variable), adds the variable name to
              `recovered_variables` to mark it as readable again.
        """

        if self.status == SiteStatus.DOWN:
            return False

        self.data_manager.write_variable(transaction, variable, value)

        idx = int(variable[1:])  
        if self.status == SiteStatus.RECOVERING and idx % 2 == 0:
            self.recovered_variables.add(variable)

        return True

    def fail(self):
        """
        Mark this site as failed / DOWN.

        Side Effects:
            - Sets the site status to SiteStatus.DOWN.
            - Clears `recovered_variables` (no variable is readable anymore).
            - Logs that the site has failed.
        """
        self.set_status(SiteStatus.DOWN)
        self.recovered_variables = set()
        log.info(f"Site {self.id} failed")

    def recover(self):
        """
        Transition this site into the RECOVERING state after a failure.

        Side Effects:
            - Populates `recovered_variables` with all odd variables that
              live on this site.
            - Sets the site status to SiteStatus.RECOVERING.
        """
        # This would make sense once we actually kill the server

        for variable in self.data_manager.variable_map.keys():

            if int(variable[1:]) % 2 != 0:
                self.recovered_variables.add(variable)

        self.set_status(SiteStatus.RECOVERING)

    def dump_site(self):
        """
        Log a human-readable snapshot of all variables on this site.

        Side Effects:
            - Writes the formatted state of this site to the logger.
        """
        log.info("=== Site " + str(self.id) + " ===")

        if self.status == SiteStatus.DOWN:
            log.info("This site is down")
            return

        count = 0
        for index in list(self.data_manager.variable_map):

            variable = self.data_manager.variable_map[index]

            if self.status == SiteStatus.RECOVERING:

                count += 1

                if variable.name not in self.recovered_variables:
                    log.info(variable.name + ":" +
                             " is not available for reading")
                else:
                    log.info(variable.name + ": " + str(variable.value) +
                             " (available at site " + str(self.id) +
                             " for reading as it is the only" +
                             " copy or has been written after recovery)")
                continue

            if variable.value != int(index[1:]) * 10:
                count += 1
                log.info(variable.name + ":  " +
                         str(variable.value) + " at site " + str(self.id))

        if count != len(self.data_manager.variable_map):
            log.info("All other variables have their initial values.")

    def get_all_variables(self):
        """
        Return a list of all Variable objects stored at this site.

        Returns:
            list[Variable]:
                List containing every Variable instance held in this site's
                DataManager.

        Side Effects:
            - None (the caller receives a new list, but the Variable objects
              themselves are shared).
        """
        variables = list()

        for idx in list(self.data_manager.variable_map):

            variable = self.data_manager.variable_map[idx]
            variables.append(variable)

        return variables
