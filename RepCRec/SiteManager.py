import logging
from .Site import Site, SiteStatus
from .Variable import Variable
from .Instruction import InstructionIO

log = logging.getLogger(__name__)


class SiteManager:
    """
    SiteManager is responsible for creating and managing all sites in the
    distributed database, and for executing site-related instructions such
    as fail, recover, and dump.

    Attributes:
        num_sites (int): Total number of sites in the system.
        sites (list[Site | None]):  List of Site objects where sites[i] is the Site with id i. Index 0 is always None to make indices 1-based.
        num_variables (int):Total number of logical variables (x1..xN) in the system.
        txn_manager (TransactionManager | None):Reference to the TransactionManager. This is set by Main after construction, and is used to abort transactions when a
            site fails. It may be None if not yet initialized.
    """

    def __init__(self, num_sites, num_variables):
        """
        Initialize the SiteManager and create all Site objects.

        Args:
            num_sites (int):
                Number of sites to create. Sites will be indexed from
                1 to num_sites.
            num_variables (int):
                Total number of logical variables (x1..xN) in the system.

        Side effects:
            - Instantiates Site objects and stores them in self.sites.
            - Initializes an empty reference to a TransactionManager
              (self.txn_manager = None); this will be filled in by Main.
        """        
        self.num_sites = num_sites
        self.sites = [None] + [Site(i) for i in range(1, num_sites + 1)]
        self.num_variables = num_variables

        self.txn_manager = None

    def tick(self, instruction):
        """
        Dispatch a single site-related instruction.

        Args:
            instruction (InstructionIO.Instruction):
                A parsed instruction object. Its type determines which
                site-related action to perform, and its parameters
                determine which site/variable to act on.

        Side effects:
            - May call Site.dump_site(), SiteManager.fail(), or
              SiteManager.recover().
            - On fail(), may indirectly abort transactions via
              TransactionManager.
        """
        params = list(instruction.get_params())

        if instruction.get_instruction_type() == InstructionIO.OP_DUMP:
            if len(params[0]) == 0:
                for site in self.sites[1:]:
                    site.dump_site()

            elif params[0][0] == 'x':
                sites = Variable.get_sites(int(params[0][1:]))
                sites = self.get_site_range(sites)

                for site in sites:
                    variables = self.sites[site].get_all_variables()

                    for variable in variables:
                        if variable.name == params[0]:
                            log.info(variable.value)

            elif len(params[0]) == 2:
                site = self.get_site(int(params[0]))
                site.dump_site()

        elif instruction.get_instruction_type() == InstructionIO.OP_FAIL:
            self.fail(int(params[0]))

        elif instruction.get_instruction_type() == InstructionIO.OP_RECOVER:
            self.recover(int(params[0]))

        return

    def _check_index_sanity(self, index):
        """
        Helper function.Validate that a site index is within the legal range.
        Args:
            index (int): The site index to validate.
        Side effects:
            - Raises ValueError if the index is out of range.
        """
        if index > self.num_sites or index <= 0:
            raise ValueError("Index must be in range %d to %d" %
                             (1, self.num_sites))

    def get_site(self, index):
        """
        Return the Site object at a given index.

        Args:
            index (int): 1-based index of the site to be returned.
        Returns:
            Site: The Site instance corresponding to the given index.
        Side effects:
             Calls _check_index_sanity and may raise ValueError if index is out of range.
        """
        self._check_index_sanity(index)
        return self.sites[index]

    def get_site_range(self, sites):
        """
        Normalize a site selector into a concrete list of site indices.

        Args:
            sites (str | int): If the value is the string 'all', this function returns
                a list of all site indices [1, 2, ..., num_sites].Otherwise, it is 
                treated as a single site index and returned as a one-element list [sites].
        Returns:
            list[int]: A list of site indices to operate on.
        """

        if sites == 'all':
            sites = range(1, self.num_sites + 1)
        else:
            sites = [sites]
        return sites

    def get_current_variables(self):
        """
        Build a consistent snapshot of readable committed values.

        Returns:
            dict[str, int]: A mapping var_name -> value for all logical variables
                that currently have at least one readable, committed replica across
                the sites. Variables with no readable replica are omitted.
        """
        snapshot = {}

        for idx in range(1, self.num_variables + 1):
            var_name = f"x{idx}"
            value = None

            for site_id in range(1, self.num_sites + 1):
                site = self.get_site(site_id)

                if site.get_status() == SiteStatus.DOWN:
                    continue

                if var_name not in site.data_manager.get_variables():
                    continue

                if var_name not in site.recovered_variables:
                    continue

                value = site.data_manager.get_variables()[var_name].get_value()
                break

            if value is not None:
                snapshot[var_name] = value

        return snapshot

    def fail(self, index):
        """
        Mark a particular site as failed and notify the TransactionManager.

        Args:
            index (int):  1-based index of the site to be failed. 
        Side effects:
            - Validates the index and may raise ValueError if invalid.
            - Marks the specified Site as DOWN by calling Site.fail().
            - Clears that site's recovered_variables.
            - If txn_manager is set, calls txn_manager.abort_transactions_on_site_failure(index)
              to abort affected transactions.
            - Writes a log entry indicating that the site failed.
        """
        self._check_index_sanity(index)
        log.info("Site " + str(index) + " failed")

        self.sites[index].fail()

        if self.txn_manager is not None:
            self.txn_manager.abort_transactions_on_site_failure(index)


    def recover(self, index):
        """
        Recover a particular site and update its internal state.

        Args:
            index (int):
                1-based index of the site to be recovered.
        Side effects:
            - Validates the index and may raise ValueError if invalid.
            - Calls Site.recover() on the selected site, which: updates 
            recovered_variables for odd-index variables, and sets the site 
            status to SiteStatus.RECOVERING.
            - Writes a log entry indicating that the site recovered.
        """

        self._check_index_sanity(index)
        log.info("Site " + str(index) + " recovered")
        self.sites[index].recover()
