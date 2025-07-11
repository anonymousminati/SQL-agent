from typing import Any, List, Optional
import asyncio
from datetime import datetime
import json
import logging
import mysql.connector
from mysql.connector import Error

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection pool (example configuration)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'finly_dashboard',
    'pool_name': 'sql_agent_pool',
    'port': 3307,
    'pool_size': 5
}

async def get_db_connection():
    """Get a database connection from the connection pool.

    Returns:
        A MySQL database connection object.

    Raises:
        Error: If the connection cannot be established.
    """
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        logger.error(f"Database connection failed: {e}")
        raise

def format_response(success: bool, data: Any = None, error: Optional[str] = None) -> dict:
    """Standardize the response format for all tool functions.

    Args:
        success: Boolean indicating if the operation was successful.
        data: The payload data to return (default: None).
        error: Error message if operation failed (default: None).

    Returns:
        dict: A standardized response dictionary with keys:
            - success (bool): Operation status
            - data (Any): Result payload
            - error (str): Error message if failed
            - timestamp (str): ISO format timestamp
    """
    return {
        'success': success,
        'data': data,
        'error': error,
        'timestamp': datetime.now().isoformat()
    }

async def list_tables(schema: Optional[str] = None) -> dict:
    """List all tables in the database or a specific schema.

    Args:
        schema: Optional schema/database name to filter tables.
                If None, uses the default database from connection.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - tables: List of table names
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await list_tables()
        {'success': True, 'data': {'tables': ['customers', 'orders']}, ...}
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = "SHOW TABLES"
        if schema:
            query = f"SHOW TABLES FROM `{schema}`"
            
        cursor.execute(query)
        tables = [table[f"Tables_in_{schema or DB_CONFIG['database']}"] for table in cursor]
        
        cursor.close()
        conn.close()
        
        return format_response(True, {'tables': tables})
        
    except Error as e:
        logger.error(f"Error listing tables: {e}")
        return format_response(False, error=str(e))

async def read_query(query: str, params: Optional[dict] = None) -> dict:
    """Execute a SELECT query and return the results.

    Args:
        query: SQL SELECT query string.
        params: Optional dictionary of parameters for parameterized queries.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - results: List of query result rows (as dicts)
                - row_count: Number of rows returned
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await read_query("SELECT * FROM customers WHERE id = %(id)s", {'id': 1})
        {'success': True, 'data': {'results': [{'id': 1, 'name': 'John'}], 'row_count': 1}, ...}
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(query, params or ())
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'results': results,
            'row_count': len(results)
        })
        
    except Error as e:
        logger.error(f"Query execution failed: {e}\nQuery: {query}")
        return format_response(False, error=str(e))

async def write_query(query: str, params: Optional[dict] = None) -> dict:
    """Execute an INSERT, UPDATE, or DELETE query.

    Args:
        query: SQL write operation query string.
        params: Optional dictionary of parameters for parameterized queries.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - affected_rows: Number of rows affected
                - lastrowid: Last inserted row ID (for INSERTs)
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await write_query("INSERT INTO customers (name) VALUES (%(name)s)", {'name': 'Alice'})
        {'success': True, 'data': {'affected_rows': 1, 'lastrowid': 5}, ...}
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(query, params or ())
        affected_rows = cursor.rowcount
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'affected_rows': affected_rows,
            'lastrowid': cursor.lastrowid
        })
        
    except Error as e:
        conn.rollback()
        logger.error(f"Write operation failed: {e}\nQuery: {query}")
        return format_response(False, error=str(e))


async def create_table(table_name: str, columns: List[dict], 
                      constraints: Optional[List[str]] = None) -> dict:
    """Create a new table with specified columns and constraints.

    Args:
        table_name: Name of the table to create.
        columns: List of column definition dictionaries with keys:
            - name: Column name
            - type: Column data type (e.g., 'VARCHAR(255)')
            - constraints: Optional column constraints (e.g., 'NOT NULL')
        constraints: Optional list of table-level constraints.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - table_name: Name of created table
                - sql: The executed CREATE TABLE statement
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> columns = [
        ...     {'name': 'id', 'type': 'INT', 'constraints': 'PRIMARY KEY AUTO_INCREMENT'},
        ...     {'name': 'name', 'type': 'VARCHAR(255)', 'constraints': 'NOT NULL'}
        ... ]
        >>> await create_table('customers', columns)
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        
        column_defs = []
        for col in columns:
            col_def = f"`{col['name']}` {col['type']}"
            if col.get('constraints'):
                col_def += f" {col['constraints']}"
            column_defs.append(col_def)
        
        # Fixed: Removed newlines from f-string
        column_defs_str = ", ".join(column_defs)
        constraints_str = ", " + ", ".join(constraints) if constraints else ""
        create_stmt = f"CREATE TABLE `{table_name}` ({column_defs_str}{constraints_str})"
        
        cursor.execute(create_stmt)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'table_name': table_name,
            'sql': create_stmt
        })
        
    except Error as e:
        conn.rollback()
        logger.error(f"Table creation failed: {e}")
        return format_response(False, error=str(e))

async def alter_table(table_name: str, alterations: List[dict]) -> dict:
    """Modify an existing table's structure.

    Args:
        table_name: Name of the table to alter.
        alterations: List of alteration dictionaries with keys:
            - action: Type of alteration ('ADD COLUMN', 'MODIFY COLUMN', 'DROP COLUMN', 'RENAME TO')
            - name: Column name (for column operations)
            - type: New column type (for ADD/MODIFY)
            - constraints: Optional column constraints
            - new_name: New table name (for RENAME TO)

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - table_name: Name of altered table
                - alterations: List of executed ALTER statements
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> alterations = [
        ...     {'action': 'ADD COLUMN', 'name': 'email', 'type': 'VARCHAR(255)'},
        ...     {'action': 'MODIFY COLUMN', 'name': 'age', 'type': 'INT', 'constraints': 'NOT NULL'}
        ... ]
        >>> await alter_table('customers', alterations)
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        
        alter_statements = []
        for alter in alterations:
            stmt = f"ALTER TABLE `{table_name}` {alter['action']}"
            if alter['action'] in ('ADD COLUMN', 'MODIFY COLUMN'):
                stmt += f" `{alter['name']}` {alter['type']}"
                if alter.get('constraints'):
                    stmt += f" {alter['constraints']}"
            elif alter['action'] == 'DROP COLUMN':
                stmt += f" `{alter['name']}`"
            elif alter['action'] == 'RENAME TO':
                stmt += f" `{alter['new_name']}`"
            
            alter_statements.append(stmt)
            cursor.execute(stmt)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'table_name': table_name,
            'alterations': alter_statements
        })
        
    except Error as e:
        conn.rollback()
        logger.error(f"Table alteration failed: {e}")
        return format_response(False, error=str(e))

async def drop_table(table_name: str, if_exists: bool = True) -> dict:
    """Permanently remove a table from the database.

    Args:
        table_name: Name of the table to drop.
        if_exists: If True, adds IF EXISTS clause to prevent errors.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - table_name: Name of dropped table
                - sql: The executed DROP statement
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await drop_table('temp_data')
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor()
        
        if_exists_clause = "IF EXISTS " if if_exists else ""
        drop_stmt = f"DROP TABLE {if_exists_clause}`{table_name}`"
        
        cursor.execute(drop_stmt)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'table_name': table_name,
            'sql': drop_stmt
        })
        
    except Error as e:
        conn.rollback()
        logger.error(f"Table drop failed: {e}")
        return format_response(False, error=str(e))

async def describe_table(table_name: str) -> dict:
    """Retrieve the structure and indexes of a table.

    Args:
        table_name: Name of the table to describe.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - table_name: Name of described table
                - structure: List of column definitions
                - indexes: List of table indexes
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await describe_table('customers')
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(f"DESCRIBE `{table_name}`")
        structure = cursor.fetchall()
        
        cursor.execute(f"SHOW INDEX FROM `{table_name}`")
        indexes = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'table_name': table_name,
            'structure': structure,
            'indexes': indexes
        })
        
    except Error as e:
        logger.error(f"Table description failed: {e}")
        return format_response(False, error=str(e))

async def export_query(query: str, format: str = 'json', 
                      params: Optional[dict] = None) -> dict:
    """Execute a query and return results in specified format.

    Args:
        query: SQL query to execute.
        format: Output format ('json' or 'csv').
        params: Optional parameters for parameterized query.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - format: The output format used
                - data: The formatted output string
                - row_count: Number of rows exported
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await export_query("SELECT * FROM customers", format='csv')
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(query, params or ())
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if format == 'json':
            output = json.dumps(results, indent=2)
        elif format == 'csv':
            if not results:
                output = ""
            else:
                headers = results[0].keys()
                output = ",".join(headers) + "\n"
                for row in results:
                    output += ",".join(str(v) for v in row.values()) + "\n"
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        return format_response(True, {
            'format': format,
            'data': output,
            'row_count': len(results)
        })
        
    except Error as e:
        logger.error(f"Export failed: {e}\nQuery: {query}")
        return format_response(False, error=str(e))
        


async def append_insight(table_name: str, insight: str, 
                        metadata: Optional[dict] = None) -> dict:
    """Store an analytical insight about a table or dataset.

    Args:
        table_name: Table this insight relates to.
        insight: Textual description of the insight.
        metadata: Optional additional metadata as dictionary.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - table_name: Name of table the insight relates to
                - insight_id: ID of the newly created insight record
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await append_insight('customers', 'Found 10 inactive customers', 
        ...                    {'severity': 'low'})
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `data_insights` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                insight TEXT NOT NULL,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX (table_name)
            )
        """)
        
        cursor.execute("""
            INSERT INTO `data_insights` (table_name, insight, metadata)
            VALUES (%s, %s, %s)
        """, (table_name, insight, json.dumps(metadata) if metadata else None))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'table_name': table_name,
            'insight_id': cursor.lastrowid
        })
        
    except Error as e:
        conn.rollback()
        logger.error(f"Insight append failed: {e}")
        return format_response(False, error=str(e))

async def list_insights(table_name: Optional[str] = None) -> dict:
    """Retrieve stored insights for a specific table or all tables.

    Args:
        table_name: Optional table name to filter insights.

    Returns:
        dict: Standardized response with:
            - success: Operation status
            - data: Dictionary containing:
                - insights: List of insight records
                - count: Total number of insights returned
            - error: Error message if failed
            - timestamp: Operation timestamp

    Example:
        >>> await list_insights('customers')
        >>> await list_insights()  # Get all insights
    """
    try:
        conn = await get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT * FROM `data_insights`"
        params = None
        
        if table_name:
            query += " WHERE table_name = %s"
            params = (table_name,)
        
        query += " ORDER BY created_at DESC"
        
        cursor.execute(query, params or ())
        insights = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return format_response(True, {
            'insights': insights,
            'count': len(insights)
        })
        
    except Error as e:
        logger.error(f"Insight listing failed: {e}")
        return format_response(False, error=str(e))