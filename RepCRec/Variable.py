import copy

class Variable:
    """
    Variable class represents the data of our sites which can be read or written by transactions

    Args:
        index: index of variable
        name: Name of the variable
        value: Initial value of the variable
        current_site_id: Index of the site on which the variable is present
    """

    def __init__(self, index, name, value, current_site_id):
        self.index = index
        self.name = name
        self.current_site_id = current_site_id
        self.value = value

    @classmethod
    def get_sites(self, id):
        """
        Class method which returns the sites on which the variable is present given an id

        Args:
            id: id of the variable for which list of sites is requested
        Returns:
            List of sites on which variable with index=id is present
        """
        if type(id) == str:
            id = int(id[1:])

        if id % 2 == 0:
            return 'all'
        else:
            return (id % 10) + 1

    def get_current_site(self):
        """
        Getter for current site

        Returns:
            Current site index of the variable
        """
        return self.current_site_id

    def get_value(self):
        """
        Getter for value

        Returns:
            Current value of the variable
        """
        return self.value

    def set_value(self, value):
        """
        Setter of variable value

        Args:
            value: Value to which the value of variable is to be set
        """
        self.value = value


    def replicate(self):
        """
        Return a deep copy of this variable

        Returns:
            Copy of this variable
        """
        return copy.deepcopy(self)
