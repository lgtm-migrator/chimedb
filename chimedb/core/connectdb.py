"""
==========================================================================
Connection handler for the CHIME database (:mod:`~chimedb.core.connectdb`)
==========================================================================

.. currentmodule:: chimedb.core.connectdb

Upon a call to `connect`, initialise connections to the CHIME database. These
can be accessed using the `current_connector()` function.  Connections are not
shared between threads.

.. warning::
    Do not call `connect` during an import statement. This will cause the
    process to hang as the ssh tunnel attempts to create threads which can't
    acquire the global import lock.

This module needs configuration data for the database connection.  Several
different configuration sources are probed for, in order.  The first source
that exists is used, even if the connection configuration supplied by that
source doesn't result in a successful connection to the database.

The configuration sources searched for are:

    * an environmental variable CHIMEDB_SQLITE, containing the path to a
        sqlite database
    * a YAML file specified by the environmental variable CHIMEDBRC
    * a YAML file called ``.chimedbrc`` in the current directory
    * a YAML file called ``.chimedbrc`` in the user's home directory
    * a YAML file located at ``/etc/chime/chimedbrc``
    * a Python module called `chimedb.config`.

If none of these configuration sources can be found, NoRouteToDatabase is
raised and the connection attempt fails.

Any of the YAML files listed above, if present, should look like:

.. code-block:: yaml

    connector:
        db:              mydb
        user:            myuser
        passwd:          mypasswd
        host:            myhost
        port:            NNNN
        tunnel_host:     mytunnelhost
        tunnel_user:     mytunneluser
        tunnel_identity: mytunnelidentity

    connector_rw:
        db:              mydb
        user:            myuser
        passwd:          mypasswd
        host:            myhost
        port:            NNNN
        tunnel_host:     mytunnelhost
        tunnel_user:     mytunneluser
        tunnel_identity: mytunnelidentity

Or for a sqlite database:

.. code-block:: yaml

    connector:
        db_type:         sqlite
        db:              mydb_file.db

(Of course, you should put proper arguments in your file! No quotation marks
should be used for strings.) If no tunnel is required, simply omit the
``tunnel_`` entries. You must define both ``connector`` and ``connector_rw``.

If none of the YAML files are found, the last thing tried is to import a
Python module called ``chimedb.config``.  This module, if present, should
provide two lists (or other iterable object) called
``chimedb.config.connectors`` and ``chimedb.config.connectors_rw``.  These
two objects should contain BaseConnector-derived objects containing the
connector data.  For both the lists (or iterables), the connectors are
tried in order until one succeeds.

Classes
=======

.. autosummary::
    :toctree: generated/

    BaseConnector
    MySQLConnector
    SqliteConnector


Constants
=========

These constants tell the module how to connect to the CHIME database.

:const:`ALL_RANKS`
    Whether to try to connect all ranks in an MPI job, or just rank=0.
"""

# === Start Python 2/3 compatibility
from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

# Explicity avoid importing int: sshtunnel requires port numbers
# be native int objects.
from future.builtins import (
    ascii,
    bytes,
    chr,
    dict,
    filter,
    hex,
    input,
    map,
    next,
    oct,
    open,
    pow,
    range,
    round,
    str,
    super,
    zip,
)  # noqa  pylint: disable=W0401, W0614
from future.builtins.disabled import *  # noqa  pylint: disable=W0401, W0614
from future.utils import raise_from

# === End Python 2/3 compatibility

import os
import logging
import MySQLdb
import peewee as pw
from playhouse import shortcuts
import socket
import sqlite3
import yaml
import sshtunnel
import threading

from .exceptions import NoRouteToDatabase, ConnectionError


# Globals
# =======

# Set module logger.
_logger = logging.getLogger("chimedb")

# Thread-local connectors: the MySQLdb module prohibits
# multiple threads from using the same database connection
# so we store the current connection in thread-local storage
_threadlocal = threading.local()

# This cannot be "localhost" because that is used as a special
# value by MySQLdb to indicate that it should connect to a local
# socket
_LOCALHOST = "127.0.0.1"

# We check these in order before trying chimedb.config
_RC_FILES = [
    os.path.join(os.curdir, ".chimedbrc"),
    os.path.join(os.path.expanduser("~"), ".chimedbrc"),
    "/etc/chime/chimedbrc",
]

# Cluster config
# =============
#
# Allow all ranks to connect to the DB
ALL_RANKS = False


def current_connector(read_write=False):
    """The current database connector for this thread, or None if no
    database connection has been made.

    Parameters
    ----------
    read_write : bool, optional
        If True, the read-write connector is returned.  If False, or
        omitted, the read-only connector to the database is returned.

    Returns
    -------
    BaseConnector-derived object or None
    """
    if read_write:
        return getattr(_threadlocal, "current_connector_RW", None)
    return getattr(_threadlocal, "current_connector", None)


def connect_this_rank():
    """Returns True if we should attempt a connection to the database
    from the current MPI rank (or if no MPI support is present).
    """
    if ALL_RANKS:
        return True

    try:
        from mpi4py import MPI

        return MPI.COMM_WORLD.Get_rank() == 0
    except ImportError:
        return True


# Database Class
# ==============


class RetryOperationalError(object):
    """Rewrite of the former `peewee.shortcuts.RetryOperationalError` mixin.

    Source: https://github.com/coleifer/peewee/issues/1472
    """

    def execute_sql(self, sql, params=None, commit=True):
        try:
            cursor = super(RetryOperationalError, self).execute_sql(
                sql, params, commit
            )
        except pw.OperationalError:
            if not self.is_closed():
                self.close()
            with pw.__exception_wrapper__:
                cursor = self.cursor()
                cursor.execute(sql, params or ())
                if commit and not self.in_transaction():
                    self.commit()
        return cursor


class MySQLDatabaseReconnect(RetryOperationalError, pw.MySQLDatabase):
    """A MySQL database class which will automatically retry connections.
    """

    pass


# Connectors
# ==========


class BaseConnector(object):
    """Abstract base class for all data base connectors.

    A connector is an object that knows how to create various types of
    connections to a given database.

    Initialization signature is subclass dependant.  Subclasses must
    re-implement get_connection and get_peewee_database.

    Attributes
    ----------
    description

    Static methods
    --------------
    from_dict

    Methods
    -------
    get_connection
    get_peewee_database

    """

    def get_connection(self):
        """Get a standard connection to the database."""
        raise NotImplementedError("BaseConnector must be subclassed.")

    def get_peewee_database(self):
        """Get :mod:`peewee` database object."""
        raise NotImplementedError("BaseConnector must be subclassed.")

    @property
    def description(self):
        """A human-readable description of this connector as a string."""
        return "Unspecified connection"

    @staticmethod
    def from_dict(d, context=None):
        """Returns a new subclass of :class:`BaseConnector` based on data in
        a dictionary.

        Typically invoked to initialise a connector from a parsed YAML file.
        """

        defaults = {
            "passwd": "",
            "tunnel_host": None,
            "tunnel_user": None,
            "tunnel_identity": None,
        }

        # Default database type is MySQL.
        if "db_type" not in d:
            d["db_type"] = "MySQL"
        if d["db_type"] == "sqlite":
            return SqliteConnector(d["db"])
        elif d["db_type"] == "MySQL":
            for var, default in defaults.items():
                if var not in d:
                    d[var] = default

            if d["tunnel_identity"]:
                d["tunnel_identity"] == os.path.expanduser(d["tunnel_identity"])

            return MySQLConnector(
                d["db"],
                d["user"],
                d["passwd"],
                d["host"],
                int(d["port"]),
                d["tunnel_host"],
                d["tunnel_user"],
                d["tunnel_identity"],
            )
        else:
            ctx = " in {0}".format(context) if context else ""
            raise ValueError(
                "Invalid database type ({0})".format(d["db_type"]) + ctx
            )


class MySQLConnector(BaseConnector):
    """Connector for MySQL databases.

    Parameters
    ----------
    db : str
        Name of the database on the server.
    user : str
        MySQL user name
    passwd : str
        MySQL password
    host : str
        MySQL server hostname.
    port : int
        TCP port of MySQL server. Default: standard port (3306).
    tunnel_host : str
        Hostname to tunnel through to connect to MySQL server using ssh port
        forwarding. Default is to not tunnel but connect directly.
    tunnel_user : str
        User name to use to log into tunnel server.
    identity_file : str
        Filename of ssh private key to use to log into tunnel server.
    """

    _tunnel = None

    def __init__(
        self,
        db,
        user,
        passwd,
        host,
        port,
        tunnel_host=None,
        tunnel_user=None,
        tunnel_identity=None,
    ):
        self._db = db
        self._user = user
        self._passwd = passwd
        self._host = host
        self._port = port
        self._tunnel_host = tunnel_host
        self._tunnel_port = None
        self._tunnel_user = tunnel_user
        self._tunnel_identity = tunnel_identity

    def get_connection(self):
        self.ensure_route_to_database()
        host, port = self._host_port()
        try:
            connection = MySQLdb.connect(
                db=self._db,
                host=host,
                port=port,
                user=self._user,
                passwd=self._passwd,
                connect_timeout=1,
            )
        except MySQLdb.OperationalError as e:
            raise ConnectionError(
                "Operational Error while connecting to database: {0}".format(e)
            )
        return connection

    def get_peewee_database(self):
        self.ensure_route_to_database()
        host, port = self._host_port()
        try:
            database = MySQLDatabaseReconnect(
                self._db,
                host=host,
                port=port,
                user=self._user,
                passwd=self._passwd,
            )
        except None:
            # TODO More descriptive here.
            raise ConnectionError("Failed to connect to database.")
        return database

    @property
    def description(self):
        out = "MySQL database at %s port %d" % (self._host, self._port)
        if self._tunnel_host:
            out += " tunnelled through {0} to localhost".format(
                self._tunnel_host
            )
            if self._tunnel_port is not None:
                out += " port {0}".format(self._tunnel_port)
        return out

    def _host_port(self):
        if self._tunnel_host:
            host = _LOCALHOST
            port = self._tunnel_port
        else:
            host = self._host
            port = self._port
        return host, port

    def ensure_route_to_database(self):
        # Check if we need a tunnel and create one if need be.
        if not self._tunnel_host or tunnel_active(self._tunnel_port):
            return

        if not connect_this_rank():
            return

        _logger.debug(
            "Attempting SSH tunnel to {0}:{1} through {2}".format(
                self._host, self._port, self._tunnel_host
            )
        )

        try:
            self._tunnel = sshtunnel.SSHTunnelForwarder(
                self._tunnel_host,
                remote_bind_address=(self._host, self._port),
                local_bind_address=(_LOCALHOST,),
                ssh_username=self._tunnel_user,
                ssh_pkey=self._tunnel_identity,
                threaded=False,  # Need to stop MySQL/sshtunnel hanging
            )
        except ValueError:
            msg = "No authentication option for %s" % self._tunnel_host
            raise NoRouteToDatabase(msg)

        # Set the threads to be daemon threads so that Python doesn't
        # hang waiting for them when it tries to exit
        self._tunnel.daemon_transport = True
        self._tunnel.daemon_forward_servers = True

        # Try to start and handle any exceptions
        try:
            self._tunnel.start()
        except (
            sshtunnel.BaseSSHTunnelForwarderError,
            sshtunnel.HandlerSSHTunnelForwarderError,
        ):
            msg = "Could not tunnel through {0}.".format(self._tunnel_host)
            raise NoRouteToDatabase(msg)

        # Get the bound port number
        self._tunnel_port = self._tunnel.local_bind_address[1]

        # Wait for the tunnel to be established
        self._tunnel.skip_tunnel_checkup = False
        self._tunnel.check_tunnels()  # This waits for the tunnel to come up
        if not self._tunnel.tunnel_is_up[(_LOCALHOST, self._tunnel_port)]:
            raise ConnetionError(
                "An error occurred while setting up the tunnel."
            )

    def __del__(self):
        # Stop the SSH tunnel
        if self._tunnel is not None:
            self._tunnel.stop()


class SqliteConnector(BaseConnector):
    """Connector for Sqlite databases.

    Parameters
    ----------
    db : str
        Filename for the Sqlite database.

    """

    def __init__(self, db):
        self._db = db

    def ensure_route_to_database(self):
        if not os.path.isfile(self._db):
            raise NoRouteToDatabase("No Sqlite database at file %s." % self._db)

    def get_connection(self):
        self.ensure_route_to_database()
        try:
            connection = sqlite3.connect(self._db)
        except sqlite3.OperationalError:
            # TODO More descriptive here.
            raise ConnectionError("Failed to connect to database.")
        return connection

    def get_peewee_database(self):
        self.ensure_route_to_database()
        try:
            database = pw.SqliteDatabase(self._db)
        except None:
            # TODO More descriptive here.
            raise ConnectionError("Failed to connect to database.")
        return database

    @property
    def description(self):
        out = "Sqlite database in file %s" % self._db
        return out


# Functions for connecting to the DB.
# ===================================


def tunnel_active(tunnel_port):
    """Returns true if a connection to local port <tunnel_port> succeeds"""
    if tunnel_port is None:
        return False

    sd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Just added this to speed things up.  I think this is enough time. -KM
    sd.settimeout(0.5)
    try:
        # Connect to the given tunnel_port on localhost.
        sd.connect((_LOCALHOST, tunnel_port))
    except socket.error:
        return False
    sd.close()
    return True


def create_tunnel(
    host,
    port,
    tunnel,
    tunnel_port,
    user=None,
    identity=None,
    ssh_port=None,
    bind_address=None,
):
    # XXX This doesn't seem to raise an error if it fails.  This is a shame.
    command = "/usr/bin/ssh -o ServerAliveInterval=60"
    if user is not None:
        command += " -l %s" % user
    if identity is not None:
        command += " -i %s" % identity
    if ssh_port is not None:
        command += " -p %i" % ssh_port

    if bind_address is None:
        command += " -fN -x -L %d:%s:%d %s" % (tunnel_port, host, port, tunnel)
    else:
        command += " -fN -x -L %s:%d:%s:%d %s" % (
            bind_address,
            tunnel_port,
            host,
            port,
            tunnel,
        )

    if not tunnel_active(tunnel_port):
        msg = (
            "SSH tunnel to DB inactive: creating one with command %s." % command
        )
        _logger.info(msg)
        # print command    # Eventually get rid of this.
        pipe = os.popen(command, "r")
        pipe.close()


def connected_mysql(db):
    try:
        db.ping()
        return True
    except MySQLdb.InterfaceError:
        return False


def close_mysql(db):
    if connected_mysql(db):
        db.commit()
        db.close()


def _initialize_connections(connectors_to_try, context, rw=False):
    for connector in connectors_to_try:
        try:
            connector.get_connection()
        except (NoRouteToDatabase, ConnectionError):
            _logger.debug(
                "Unable to connect to {0} defined by {1}.".format(
                    connector.description, context
                )
            )
            continue
        if rw:
            msg_conn = "Read-write"
            _threadlocal.current_connector_RW = connector
        else:
            msg_conn = "Read-only"
            _threadlocal.current_connector = connector
        _logger.info(
            "{0} connection to {1} defined by {2} established.".format(
                msg_conn, connector.description, context
            )
        )
        break
    else:
        _logger.warning("Could not establish connection to CHIME database.")


def _try_rc_files():
    conn = dict()
    sections = ("connector", "connector_rw")

    # Try the contents of CHIMEDBRC first, if given
    if "CHIMEDBRC" in os.environ and os.environ["CHIMEDBRC"]:
        _RC_FILES.insert(0, os.environ["CHIMEDBRC"])

    for rc_file in _RC_FILES:
        try:
            with open(rc_file) as f:
                rc = yaml.safe_load(f)

                # Nothing in this file, so skip it
                if rc is None:
                    continue

                # Create the connectors
                for section in sections:
                    if section in rc:
                        conn[section] = BaseConnector.from_dict(
                            rc[section], rc_file
                        )

                # It's all or nothing
                if "connector" in conn:
                    if "connector_rw" not in conn:
                        raise ConnectionError(
                            """
Asymmetric connection configuration!
File "{0}" defines 'connector' but not 'connector_rw'.""".format(
                                rc_file
                            )
                        )
                    else:
                        return (
                            [conn["connector"]],
                            [conn["connector_rw"]],
                            rc_file,
                        )
                elif "connector_rw" in conn:
                    raise ConnectionError(
                        """
Asymmetric connection configuration!
File "{0}" defines 'connector_rw' but not 'connector'.""".format(
                            rc_file
                        )
                    )

                # If we got here, neither connector was created, so we try the
                # next file
                logging.debug("Skipping invalid RC file {0}".format(rc_file))
        except IOError:
            pass

    # No valid file found
    return None


def connect(reconnect=False):
    """Connect to the CHIME Database.

    .. warning::
        Do not call this routine (or other routines that call it) during an
        import statement. This will cause the process to hang as the ssh tunnel
        attempts to create threads which can't acquire the global import lock.

    Parameters
    ----------
    reconnect : bool, optional
        Re-establish a connection even if one already exists.
    """

    # Initialise connections (only rank=0) if batch job.
    if not connect_this_rank():
        return

    current_connector = getattr(_threadlocal, "current_connector", None)
    current_connector_RW = getattr(_threadlocal, "current_connector_RW", None)

    if not reconnect and (
        current_connector is not None or current_connector_RW is not None
    ):
        _logger.debug("Connection already exists.")
        return

    # First look for CHIMEDB_SQLITE
    if "CHIMEDB_SQLITE" in os.environ and os.environ["CHIMEDB_SQLITE"]:
        connectors = [SqliteConnector(os.environ["CHIMEDB_SQLITE"])]
        connectors_rw = [SqliteConnector(os.environ["CHIMEDB_SQLITE"])]
        context = "CHIMEDB_SQLITE"
    else:
        rc_data = _try_rc_files()

        if rc_data:
            connectors, connectors_rw, context = rc_data
        else:
            try:
                from chimedb.config import connectors, connectors_rw
            except ImportError:
                raise_from(
                    NoRouteToDatabase(
                        """
Unable to find connection configuration for the database!
Either provide a chimedb RC file in one of the default
locations or install `chimedb.config`."""
                    ),
                    None,
                )
            else:
                context = "chimedb.config"

    # Try to connect, these will raise  ConnectionError on error
    _initialize_connections(connectors, context)
    _initialize_connections(connectors_rw, context, True)

    # If that succeeded, remember the connectors
    current_connector = getattr(_threadlocal, "current_connector", None)
    current_connector_RW = getattr(_threadlocal, "current_connector_RW", None)

    if current_connector is None or current_connector_RW is None:
        raise ConnectionError()
