class BaseRecords():
    """Record table holding data information

    This class provides record table management:
        - create record database
        - migrate schema
        - get information
        - query record database

    # Args:
        record_path <str>: place holding the record table
        record_config <{}>: configuration defining the config
    """

    def __init__(self, record_path, record_config):
        """Initialize the record"""
        self._record_path = record_path
        self._record_config = record_config

    def load_record_db_into_dict(self):
        """Load record DB into dictionary

        # Returns:
            <{id: {cols: values}}>: the record database
        """
        raise NotImplementedError('must implement')

    def create_index_db(self):
        """Create SQL database

        # Returns:
            <[str]>: list of column names
        """
        raise NotImplementedError('must implement')

    def is_existed(self):
        """Check if the record exists"""
        raise NotImplementedError('must implement')

    def get_record_commit(self):
        """Get the commit hash that the index database points to

        # Returns
            <str>: the commit hash that index database points to. None if nothing
        """
        raise NotImplementedError('must implement')
