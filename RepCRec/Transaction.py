from enum import Enum

class TransactionStatus(Enum):
    """
    Status of a transaction.

    RUNNING : the transaction is active and may read/write.
    ABORTED : the transaction has been rolled back and will do nothing else.
    COMMITTED : the transaction has successfully committed.
    """
    RUNNING = 0
    ABORTED = 1
    COMMITTED = 2


class Transaction:
    """
    Represents a single transaction.

    Args:
        id (int): Internal numeric id for this transaction.
        name (str): External transaction name.

    Side effects:
        Methods of this class only modify the in-memory fields of
        this Transaction instance. They do not directly modify any
        site or variable; actual commits are performed by the
        TransactionManager.
    """

    def __init__(self, id, name):
        """
        Initialize a new Transaction object.

        Args:
            id (int): Internal numeric identifier for this transaction.
            name (str): External name of the transaction (e.g., "T1").

        Initializes:
            status (TransactionStatus): Starts as RUNNING.
            snapshot (dict): Snapshot of committed variable values at begin().
            uncommitted_variables (dict): Write-set buffer (var → new value).
            read_set (set): Variables read by this transaction.
            read_variables (dict): Detailed read history (var → list of values).
            write_set (set): Variables written by this transaction.
            start_ts (int | None): Timestamp when the transaction began.
            commit_ts (int | None): Timestamp when the transaction committed.
            write_sites (set): Sites that this transaction has written to.
        """
        self.status = TransactionStatus.RUNNING
        self.id = id
        self.sites_accesssed = []
        self.name = name

        self.snapshot = dict()                
        self.uncommitted_variables = dict()    

        self.read_set = set()           
        self.read_variables = dict()         
       
        self.write_set = set()

        self.start_ts = None
        self.commit_ts = None

        self.write_sites = set()
        
    def get_id(self):
        """
        Get id of the transaction

        Returns:
            Id of the transaction
        """
        return self.id

    def get_status(self):
        """
        Get staus of the transaction

        Returns:
            Status of the transaction
        """
        return self.status


    def get_sites_accessed(self):
        """
        Gets sites accessed by the transaction

        Returns:
            List of sites accessed
        """
        return self.sites_accessed

    def set_status(self, status):
        """
        Set status of the transaction

        Args:
            status: TransactionStatus type to be set to this transaction's
                    status
        Raises:
            ValueError if unknown transactionstatus type is passed
        """
        if status in TransactionStatus:
            self.status = status
        else:
            raise ValueError("TransactionStatus is not valid")
            return

    def get_read_variables(self):
        """
        Get all the read variables of this transaction

        Returns:
            Dict of variables read
        """
        return self.read_variables

    def get_uncommitted_variables(self):
        """
        Get all the variables wrote by this transaction

        Returns:
            Dict of variables wrote
        """
        return self.uncommitted_variables

    def clear_uncommitted_variables(self):
        """
        Clear the uncommmitted variable of the transaction
        """
        self.uncommitted_variables = dict()

    def __eq__(self, other):
        """
        Compare this transaction with some other

        Returns:
            Boolean: Whether equal or not
        """
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False
