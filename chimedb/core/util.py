import logging

from .orm import base_model, name_table

# Routines for setting up the database
# =====================================


def check_tables(create=False, abstract_tables=[]):
    """Check if the tables in the database exist (and create if requested).

    Parameters
    ----------
    create : boolean, optional
        Create the tables if they don't exist.
    abstract_tables : list, optional
        A list of chimedb.core.orm.base_model subclasses to ignore, if any

    Returns
    -------
    missing_tables : list
        The list of missing tables.
    """

    # Fetch subclasses (i.e. tables)
    tables = base_model.__subclasses__()

    abstract_tables.append(name_table)

    missing_tables = []

    for tab in tables:
        if tab not in abstract_tables and not tab.table_exists():
            if create:
                logging.info("Creating table %s" % repr(tab))
                tab.create_table()
            else:
                logging.info("Missing table %s" % repr(tab))
                missing_tables.append(tab)

    return missing_tables
