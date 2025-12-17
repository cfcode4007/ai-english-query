# © 2025 Colin Bond
# All rights reserved.

import mysql.connector
from mysql.connector import Error
from typing import Optional, List, Dict, Any, Generator
import logging
from contextlib import contextmanager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MariaDBConnection:
    """
    MariaDB connection class using mysql-connector-python
    Designed to accept raw SQL strings directly
    """
    version = "0.0.2"
    # - Version 0.0.2: Added properties to expose configured host, user and database, as well as stopped automatic reconnection attempts

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 3306,
        user: str = 'root',
        password: str = '',
        database: str = None,  
        autocommit: bool = True,
        charset: str = 'utf8mb4',
        reconnect_attempts: int = 3,
        reconnect_delay: int = 2
    ):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'autocommit': autocommit,
            'charset': charset,
            'use_unicode': True,
            'get_warnings': True,
            'raise_on_warnings': False,
            'connection_timeout': 10
        }
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_delay = reconnect_delay
        self.connection = None
        self.cursor = None

    def _connect(self) -> bool:
        """Internal method to establish connection with retry logic"""        
        try:
            if self.connection and self.connection.is_connected():
                return True
                
            if self.connection:
                self.connection.close()
                
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to MariaDB at {self.config['host']}:{self.config['port']}")
            return True
            
        except Error as err:
            logger.error(f"Connection failed: {err}")
            return False
        
        # import time
        
        # for attempt in range(self.reconnect_attempts + 1):
        #     try:
        #         if self.connection and self.connection.is_connected():
        #             return True
                    
        #         if self.connection:
        #             self.connection.close()
                    
        #         self.connection = mysql.connector.connect(**self.config)
        #         self.cursor = self.connection.cursor()
        #         logger.info(f"Connected to MariaDB at {self.config['host']}:{self.config['port']}")
        #         return True
                
        #     except Error as err:
        #         logger.error(f"Connection attempt {attempt + 1} failed: {err}")
        #         if attempt < self.reconnect_attempts:
        #             time.sleep(self.reconnect_delay)
        #         else:
        #             logger.critical("All connection attempts failed.")
        #             return False
        # return False
            

    @contextmanager
    def get_cursor(self):
        """Context manager for cursor - auto-reconnect + cleanup"""
        if not self._connect():
            raise ConnectionError("Failed to connect to MariaDB")
            
        try:
            yield self.cursor
        except Error as err:
            logger.error(f"Database error: {err}")
            self.connection.rollback() # pyright: ignore[reportOptionalMemberAccess]
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            self.connection.rollback()
            raise
        finally:
            # Cursor is managed by connection, no need to close explicitly
            pass

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute a raw SQL query (SELECT or SHOW, etc.) and return list of dicts.
        For queries that modify data, use execute_modify().
        """
        if not sql.strip():
            raise ValueError("Empty SQL statement")
            
        with self.get_cursor() as cursor:
            cursor.execute(sql)
            if cursor.with_rows and cursor.description:
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                logger.info(f"Query executed successfully. Returned {len(results)} rows.")
                return results
            else:
                # Non-SELECT but has no result set (e.g., SHOW)
                return []

    def execute_modify(self, sql: str) -> int:
        """
        Execute INSERT, UPDATE, DELETE, etc. Returns number of affected rows.
        """
        if not sql.strip():
            raise ValueError("Empty SQL statement")
            
        with self.get_cursor() as cursor:
            cursor.execute(sql)
            self.connection.commit()
            affected = cursor.rowcount # type: ignore
            logger.info(f"Modification query executed. Affected rows: {affected}")
            return affected

    def query_generator(self, sql: str, chunk_size: int = 1000) -> Generator[Dict[str, Any], None, None]:
        """
        For very large result sets - streams results
        """
        with self.get_cursor() as cursor:
            cursor.execute(sql)
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                columns = [desc[0] for desc in cursor.description]
                for row in rows:
                    yield dict(zip(columns, row))

    def close(self):
        """Close connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MariaDB connection closed.")

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # 0.0.2
    @property
    def user(self) -> Optional[str]:
        """Return the name of the database this connection is configured to use.

        This first prefers the configured value passed to the constructor
        (self.config['database']). If the underlying connection is already
        established and exposes a `database` attribute, that value is used as
        a fallback.
        """
        user = self.config.get('user')
        if user:
            return user
        if self.connection and hasattr(self.connection, 'user'):
            try:
                return getattr(self.connection, 'user')
            except Exception:
                # Be conservative in what we return; if introspection fails,
                # return None rather than raising.
                return None
        return None
    
    @property
    def host(self) -> Optional[str]:
        """Return the name of the database this connection is configured to use.

        This first prefers the configured value passed to the constructor
        (self.config['database']). If the underlying connection is already
        established and exposes a `database` attribute, that value is used as
        a fallback.
        """
        host = self.config.get('host')
        if host:
            return host
        if self.connection and hasattr(self.connection, 'host'):
            try:
                return getattr(self.connection, 'host')
            except Exception:
                # Be conservative in what we return; if introspection fails,
                # return None rather than raising.
                return None
        return None

    @property
    def database(self) -> Optional[str]:
        """Return the name of the database this connection is configured to use.

        This first prefers the configured value passed to the constructor
        (self.config['database']). If the underlying connection is already
        established and exposes a `database` attribute, that value is used as
        a fallback.
        """
        db = self.config.get('database')
        if db:
            return db
        if self.connection and hasattr(self.connection, 'database'):
            try:
                return getattr(self.connection, 'database')
            except Exception:
                # Be conservative in what we return; if introspection fails,
                # return None rather than raising.
                return None
        return None
    
