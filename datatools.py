"""All needed classes for this module.

    Includes:
    * Database Connectors: Postgre and Cassandra
    * LoadDataFromCSV
    * LoadDataToCassandra
    * LoadDataToPostgre
"""

import gc
import logging
import sys
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict

import pandas as pd
import psycopg2
from cassandra import OperationTimedOut  # , ConsistencyLevel,
from cassandra.cluster import Cluster as CassandraCluster
from cassandra.cluster import ConnectionShutdown, NoHostAvailable
from cassandra.cluster import Session as CassandraSession
from cassandra.policies import (
    HostFilterPolicy,
    RoundRobinPolicy,
    WhiteListRoundRobinPolicy,
)
from cassandra.query import BatchStatement
from psycopg2._psycopg import connection as PostgreConnection
from psycopg2._psycopg import cursor as PostgreCursor
from psycopg2.extensions import register_adapter as psycopg2_register_adapter
from psycopg2.extras import Json as psycopg2_Json
from psycopg2.extras import execute_values as psycopg2_execute_values

psycopg2_register_adapter(dict, psycopg2_Json)

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class ConnectDB(ABC):
    """Connect from different DBs as ABSTRACT."""

    table_name = None  # could be string or dict

    def __init__(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute(self, command):
        pass

    @abstractmethod
    def disconnect(self):
        pass


class PostgreConnector(ConnectDB):
    """Connector for PostgreSQL.

    Try to simplify the connection and execution for PostgreSQL DB.
    Add `psycopg2_execute_values` function to the executing functions
    """

    conn: PostgreConnection
    cursor: PostgreCursor
    err: Exception

    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.cursor = None
        self.conn = None

    def connect(self):
        if self.cursor is not None:
            log.debug("cursor is not None, try to shut down first.")
            self.disconnect()

        try:
            self.conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            # if using with commit or rollback, set this to TRUE
            # self.conn.autocommit = True
        except psycopg2.OperationalError as op_error:
            self.print_exception(op_error)
            self.err = op_error
        except Exception:
            self.err = Exception("Could not connect to database.")

        if self.conn is None:
            raise self.err

        self.cursor = self.conn.cursor()

    def execute(self, command, data=None):
        if self.cursor is None:
            self.connect()
        result = None
        try:
            result = self.cursor.execute(command, data)
            self.conn.commit()
        except Exception as err:
            self.print_exception(err)
            self.conn.rollback()
        return result

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def execute_values(self, sql, data, BATCH_SIZE=None):
        """Execute a list of values.

        https://www.psycopg.org/docs/extras.html#psycopg2.extras.execute_values
        This function actually is equivalent to

        args_str = b','.join(cursor.mogrify("(%s,%s,%s)", x) for x in data)
        args_str = args_str.decode()
        cursor.execute("INSERT INTO person_data VALUES " + args_str)

        Parameters
        ----------
        sql : str
            "INSERT INTO test (id, col1, col2) VALUES %s"
            Only one "%s" in the sql
        data : a list of tuples of values
            [(1, 2, 3), (4, 5, 6), (7, 8, 9)])
        """
        if self.cursor is None:
            self.connect()

        result = None
        try:
            if BATCH_SIZE is None:
                result = psycopg2_execute_values(self.cursor, sql, data)
            else:
                i = 0
                while i < len(data):
                    result = psycopg2_execute_values(
                        self.cursor, sql, data[i : i + BATCH_SIZE]
                    )
                    i += BATCH_SIZE
            self.conn.commit()
        except Exception as err:
            self.print_exception(err)
            self.err = err
            self.conn.rollback()
        return result

    def print_exception(self, err):
        # get details about the exception
        err_type, err_obj, tb = sys.exc_info()

        print("psycopg2 ERROR:", err, "on line number:", tb.tb_lineno)
        print("psycopg2 traceback:", tb, "-- type:", err_type)

        print("extensions.Diagnostics:", err.diag)
        print("pgerror:", err.pgerror, " - pgcode:", err.pgcode)

    def disconnect(self):
        if self.cursor is not None:
            try:
                self.cursor.close()
            except Exception:
                log.debug("Can not close cursor.")
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                log.debug("Can not shut down.")


class CassandraConnector(ConnectDB):
    """Connector for Cassandra.

    This is the simplified connector for the Cassandra in a cluster.
    There are other configuration options available such as:
        * auth_provider
        * get_load_balancing_policy
    """

    session: CassandraSession
    cluster: CassandraCluster

    def __init__(self, ips, keyspace, table_name):
        self.ips = ips
        self.keyspace = keyspace
        self.table_name = table_name
        self.session = None
        # auth_provider = PlainTextAuthProvider(username=username, password=password)
        # cluster = CassandraCluster(nodes,auth_provider=auth_provider)
        # https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/auth/#cassandra.auth.PlainTextAuthProvider

    def connect(self):
        err = None
        if self.session is not None:
            log.debug("session is not None, try to shut down first.")
            self.disconnect()

        try:
            self.cluster = CassandraCluster(
                self.ips,
                load_balancing_policy=self.get_load_balancing_policy(),
            )
            self.session = self.cluster.connect(wait_for_all_pools=False)
            self.create_keyspace(self.keyspace)
            self.set_keyspace(self.keyspace)

        except (OperationTimedOut, NoHostAvailable, ConnectionShutdown) as e:
            print(e)
            err = e

        if self.session is None:
            raise err

    def execute(self, command):
        result = None
        if self.session is None:
            self.connect()

        try:
            result = self.session.execute(command)
        except ConnectionShutdown:
            self.connect()
            result = self.session.execute(command)
        return result

    def prepare(self, command):
        result = None
        try:
            result = self.session.prepare(command)
        except ConnectionShutdown:
            self.connect()
            result = self.session.prepare(command)
        return result

    def get_load_balancing_policy(self):
        return WhiteListRoundRobinPolicy([self.ips[0]])

        # below is the use of whitelist
        def whitelist_address(host):
            return host.address != self.ips[0]

        return HostFilterPolicy(
            child_policy=RoundRobinPolicy(), predicate=whitelist_address
        )

    def create_keyspace(self, keyspace, replication=None):
        if replication is None:
            replication = """WITH replication={
                                'class': 'SimpleStrategy',
                                'replication_factor' : 3
                             }"""
        q = f"CREATE KEYSPACE IF NOT EXISTS {keyspace} " + replication + ";"
        return self.execute(q)

    def set_keyspace(self, keyspace):
        return self.execute(f"USE {keyspace};")

    def disconnect(self):
        try:
            self.cluster.shutdown()
        except Exception:
            log.debug("Can not shut down.")


class DBWrapper:
    """Wapper class for different databases.

    Example of using DBWrapper to connect to PostgreSQL:

    db = "postgres"
    db_info = {"database":"postgres", "user":'postgres',
        "password":'mysecretpassword', "host":'192.168.137.101', "port":"5432"}

    db_connector = DBWrapper(db, db_info)

    result = db_connector.execute("select * from person_data where id=12;")
    print(db_connector.fetchone())

    # create random data for batch (large size) INSERT query
    import random
    start = 1300001
    num = 10000
    arr = []
    names = ('Ann', 'Bill', 'Cindy', 'Diane', 'Emma')
    for i in range(start, start + num):
        arr.append((i, random.choice(names) ,random.randint(20, 90)))

    db_connector.execute_values("INSERT INTO person_data VALUES %s", arr)
    """

    def __init__(self, db: str, info: dict):
        """Initialize the database connection.

        Parameters
        ----------
        db : str
            name of the database
        info : dict
            each database has diffrent required info
        """
        if db == "postgres":
            self.db = PostgreConnector(
                database=info["database"],
                user=info["user"],
                password=info["password"],
                host=info["host"],
                port=info["port"],
            )
        elif db == "cassandra":
            self.db = CassandraConnector(
                ips=info["ips"],
                keyspace=info["keyspace"],
                table_name=info["table_name"],
            )

        self.table_name = info["table_name"]

    def __getattr__(self, name: str):
        return getattr(self.db, name)


class LoadDataFromCSV:
    """Load big size of dataset from CSV file in `pandas` DataFrame format."""

    def load_all(self, file_path, dtype=None, chunksize=10**7):
        """Load all at once."""
        if dtype is None:
            dtype = {}
        return pd.concat(
            pd.read_csv(file_path, dtype=dtype, chunksize=chunksize)
        )

    def load_by_chunk(
        self,
        file_path,
        dtype=None,
        chunksize=10**6,
        encoding="utf-8",
        skiprows: int = None,
        fillna=None,
    ):
        """Generate data by chunk as an Iterator/Generator.

        Parameters
        ----------
        file_path : str or pathlib.Path
            Full path to the CSV file
        dtype : dict, optional
            {col:data_type}, by default None
        chunksize : int, optional
            Size (number of row) for each reading iteration, by default 10**6
        encoding : str, optional
            The encode of CSV file, by default "utf-8"
        skiprows : int, optional
            If you want to skip number of row, set with a int, by default None.
            Note, in the program, skiprows become a range which igore 1st row
            as a header
        fillna : dict, optional
            Depending on different col, fill with different value. For example:
            fillna = {
            "date_type": "event_time",  # pd.to_datetime(chunk[col])
            "category_code": "NO_CODE",  # text
            "price": 0.0,  # number / float
            "user_session": "uuid()",  # str(uuid.uuid4())
            }
            By default None

        Yields
        ------
        pd.DataFrame
            Generate dataframe from CSV file based on chunksize
        """
        if skiprows is not None:
            skiprows = range(1, skiprows)  # convert number to range

        if chunksize == -1:
            return pd.read_csv(
                file_path, dtype=dtype, encoding=encoding, skiprows=skiprows
            )

        if dtype is None:
            dtype = {}

        with pd.read_csv(
            file_path,
            dtype=dtype,
            chunksize=chunksize,
            encoding=encoding,
            skiprows=skiprows,
        ) as reader:
            for chunk in reader:
                # deal with missing values
                if fillna is not None:
                    for col, value in fillna.items():
                        if col == "date_type":
                            chunk[value] = pd.to_datetime(chunk[value])
                        else:
                            if value == "uuid()":
                                value = str(uuid.uuid4())
                            chunk[col].fillna(value, inplace=True)

                yield chunk

                del chunk
                gc.collect()  # cleaning memory


class LoadDataToCassandra(LoadDataFromCSV):
    """Load data from CSV files for eCommerce Project."""

    prepared_quey: str

    def prepare_cassandra(self, conn: ConnectDB):
        """Handle Database connection, create if not exist.

        Handle Cassandra connection, create if not exist keyspace and
         table queries
        """
        table_create_query = f"""
CREATE TABLE IF NOT EXISTS {conn.table_name} (
event_time timestamp,
event_type text,
product_id text,
category_id text,
category_code text,
brand text,
price float,
user_id text,
user_session uuid,
PRIMARY KEY (user_id, user_session, product_id)
);
"""

        self.prepared_quey = f"""
INSERT INTO {conn.table_name} (event_time, event_type, product_id, category_id,
category_code, brand, price, user_id, user_session) VALUES (?,?,?,?,?,?,?,?,?)"
"""
        conn.execute(table_create_query)

    def save_to_cassandra(
        self,
        conn,
        file_path,
        dtype=None,
        BATCH_SIZE=100,
        CHUNK_SIZE=10**6,
        SKIP_ROWS=None,
    ):
        """Save extracted data to Cassandra.

        First, we iterate data and batch inserts by using
            load_by_chunk to load Data from CSV
        Last, we insert each grouped data into the coresponding table.

        Parameters
        ----------
        conn : Database connection
            Cassandra connection by
            cassandra_conn = DBWrapper("cassandra", info) or CassandraConnector
        file_path : str or pathlib.Path
            Full path to the CSV file
        dtype : dict, optional
            {col:data_type}, by default None
        BATCH_SIZE : int, optional
            The size of a batch when inserting into Cassandra, by default 100
        CHUNK_SIZE : int, optional
            Size (number of row) for each reading iteration, by default 10**6
        SKIP_ROWS : int, optional
            If you want to skip number of row, set with a int, by default None.
            Note, in the program, skiprows become a range which igore 1st row
            as a header
        """
        # Extract information from dataframe
        # Start connecting to Cassandra
        conn.connect()
        # Handle Cassandra connection, create if not exist
        self.prepare_cassandra(conn)

        # Tracking number of rows already processed (inserted to DB)
        rows_number = 0 if SKIP_ROWS is None else SKIP_ROWS
        # Handle missing data
        fillna = {
            "date_type": "event_time",
            "category_code": "NO_CODE",
            "category_id": "0",
            "price": 0.0,
            "user_session": "uuid()",
            "brand": "NO_BRAND",
        }
        for df in self.load_by_chunk(
            file_path,
            dtype=dtype,
            chunksize=CHUNK_SIZE,
            skiprows=SKIP_ROWS,
            fillna=fillna,
        ):
            log.debug("%s Current # of rows = %s", file_path, rows_number)

            rows_number += CHUNK_SIZE
            # save all to Cassandra
            # log.debug(f" Inserting into Cassandra")

            insert_data = None
            batch = None
            i = 0
            for _, row in df.iterrows():
                if insert_data is None:
                    insert_data = conn.prepare(self.prepared_quey)
                    batch = BatchStatement()

                batch.add(
                    insert_data,
                    (
                        row.event_time.to_pydatetime(),
                        row.event_type,
                        row.product_id,
                        row.category_id,
                        row.category_code,
                        row.brand,
                        row.price,
                        row.user_id,
                        uuid.UUID(row.user_session),
                    ),
                )
                if i % BATCH_SIZE == 0:
                    conn.execute(batch)
                    insert_data = None
                    batch = None
                    gc.collect()

                i += 1

            if i % BATCH_SIZE != 0:
                # add the remaining batch into database
                conn.execute(batch)

            # Clean up to release resources (memory)
            del df
            gc.collect()

        # Clean up
        # conn.disconnect()  # disconnect externally
        log.debug(" Finished Inserting into Cassandra")
        gc.collect()


class LoadDataToPostgre(LoadDataFromCSV):
    """Load data from CSV files for eCommerce Project."""

    def prepare_database(self, conn):
        """Handle Database connection, create if not exist.

        Handle Database connection, create if not exist keyspace and
        table queries. For example, using PostgreSQL.

        We don't need CREATE SCHEMA here for sake of simplicity.

        We will create tables

        * category: category_id, category_code
        * product: product_id, category_id, brand, price
        * user: user_id, product_preferences
        * order: order_id, order_time, user_id, order_list JSON

        For order_list, we use JSON data type to store the list of purchases
            like: product_id:[price_1, price_2, price_3...]
            each product_id might have prices repeated as many times
            with the same price or different (for example, promotion for only
            1st product). JSON writes fast but reads slow.
            We also can use `hstore` but we must install an extension
            `CREATE EXTENSION hstore;`. The data, however, is in string format
            and cannot store numbers or booleans and no nested key-value pairs.

        """
        table_category_create_query = f"""
CREATE TABLE IF NOT EXISTS {conn.table_name["table_category"]} (
category_id text PRIMARY KEY,
category_code text
);
"""
        conn.execute(table_category_create_query)

        table_product_create_query = f"""
CREATE TABLE IF NOT EXISTS {conn.table_name["table_product"]} (
product_id text PRIMARY KEY,
category_id text,
brand text,
price float
);
"""
        conn.execute(table_product_create_query)

        table_user_create_query = f"""
CREATE TABLE IF NOT EXISTS {conn.table_name["table_user"]} (
user_id text PRIMARY KEY,
product_preferences text
);
"""
        conn.execute(table_user_create_query)

        table_order_create_query = f"""
CREATE TABLE IF NOT EXISTS {conn.table_name["table_order"]} (
order_id uuid PRIMARY KEY,
order_time timestamp,
user_id text,
order_list JSON
);
"""
        conn.execute(table_order_create_query)

    def save_to_database(
        self,
        conn,
        file_path,
        dtype=None,
        BATCH_SIZE=None,
        CHUNK_SIZE=10**6,
        SKIP_ROWS=None,
    ):
        """Save extracted data to database.

        In here, we use PostgreSQL to save the data.
        First, we extract and group data into different tables by using
            load_by_chunk to load Data from CSV
        Last, we insert each grouped data into the coresponding table.

        """
        # Extract information from dataframe
        category_ids = {}  # category_id:category_code
        category_codes = set()  # only category_code
        user_ids = set()  # user_id
        product_ids = {}  # product_id: [product_id, category_id, brand, price]
        # orders: {
        #    order_id: {
        #        "order_time": timestamp,
        #        "user_id" : xxx,
        #        "order_list": {
        #            "product_id" : [price_1, price_2, ...]
        #         }
        #    }
        # }
        orders = {}

        # Tracking number of rows already processed (inserted to DB)
        # If the program was shutdown in the middle of the process, then skip
        #   already processed rows (set in SKIP_ROWS)
        rows_number = 0 if SKIP_ROWS is None else SKIP_ROWS
        # Handle missing data
        fillna = {
            "category_code": "NO_CODE",
            "category_id": "0",
            "price": 0.0,
            "user_session": "uuid()",
        }

        for df in self.load_by_chunk(
            file_path,
            dtype=dtype,
            chunksize=CHUNK_SIZE,
            skiprows=SKIP_ROWS,
            fillna=fillna,
        ):
            log.debug("%s Current # of rows = %s", file_path, rows_number)

            rows_number += CHUNK_SIZE
            # save all to Cassandra
            # log.debug(f" Inserting into Cassandra")
            category_codes.update(df.category_code.unique())
            product_ids.update(
                dict(
                    zip(df.product_id, zip(df.category_id, df.brand, df.price))
                )
            )
            user_ids.update(df.user_id.unique())
            category_ids.update(dict(zip(df.category_id, df.category_code)))

            # group all purchased products by user_session
            purchase_df = df.loc[df.event_type == "purchase"]
            purchase_df = purchase_df.groupby("user_session")
            order_df = purchase_df.aggregate(lambda x: [list(x)])
            purchase_df = (
                order_df["event_time"]
                + order_df["user_id"]
                + order_df["product_id"]
                + order_df["price"]
            )
            for order_id, row in purchase_df.iteritems():
                orders[order_id] = {}  # new order by order_id = user_session
                orders[order_id]["order_time"] = row[0][0]  # keep 1st record
                orders[order_id]["user_id"] = row[1][0]
                orders[order_id]["order_list"] = defaultdict(list)
                for product_id, price in zip(row[2], row[3]):
                    orders[order_id]["order_list"][product_id].append(price)

            # Clean up to release resources (memory)
            del df
            gc.collect()

        # insert into Database, for example, PostgreSQL
        conn.connect()
        # Prepare database before inserting into.
        self.prepare_database(conn)

        # Insert into Database
        # category: category_id, category_code
        conn.execute_values(
            f"INSERT INTO {conn.table_name['table_category']}"
            " (category_id, category_code) VALUES %s ON CONFLICT DO NOTHING",
            list(category_ids.items()),
            BATCH_SIZE,
        )

        # product: product_id, category_id, brand, price
        conn.execute_values(
            f"INSERT INTO {conn.table_name['table_product']} "
            " (product_id, category_id, brand, price) VALUES %s  "
            " ON CONFLICT DO NOTHING",
            [(a,) + b for a, b in product_ids.items()],
            BATCH_SIZE,
        )

        # user: user_id, product_preferences
        conn.execute_values(
            f"INSERT INTO {conn.table_name['table_user']} (user_id) VALUES %s"
            " ON CONFLICT DO NOTHING",
            [(u,) for u in user_ids],
            BATCH_SIZE,
        )

        # order: order_id, order_time, user_id, order_list JSON
        conn.execute_values(
            f"INSERT INTO {conn.table_name['table_order']} ("
            "order_id, order_time, user_id, order_list) VALUES %s"
            " ON CONFLICT DO NOTHING",
            [
                (k, v["order_time"], v["user_id"], v["order_list"])
                for k, v in orders.items()
            ],
            BATCH_SIZE,
        )
        # clean up
        # conn.disconnect() # disconnect externally
        log.debug(" Finished Inserting into Cassandra")
