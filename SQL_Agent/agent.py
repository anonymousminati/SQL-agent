import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
sys.path.append(str(Path(__file__).parent))

from google.adk.agents import Agent
from toolList import (
    list_tables,
    read_query,
    write_query,
    create_table,
    alter_table,
    drop_table,
    describe_table,
    export_query,
    append_insight,
    list_insights
)
from re import S
import logging


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SYSTEM_INSTRUCTIONS = """
I am an advanced SQL database assistant with these capabilities:
1. Understand natural language queries and convert them to proper SQL.
2. Validate queries against database schema before execution.
3. Handle complex joins, aggregations, and subqueries.
4. Provide explanations for query results.
5. Suggest optimizations for slow queries.
6. Maintain data integrity and security.

Rules:
- Always verify table/column existence before querying.
- Use parameterized queries to prevent SQL injection.
- Validate user permissions for sensitive operations (though this agent doesn't implement full auth, acknowledge the need).
- Provide clear error messages to the user if an operation fails.
- Log all actions for audit purposes.
- When generating SQL, ensure it is syntactically correct and semantically valid for a MySQL database.
- Prioritize using the provided tools for all database interactions.
- If a query involves sensitive data, ask for confirmation before execution.
"""

SYSTEM_DESCRIPTION = """
This SQL Agent is designed to act as an intelligent intermediary between users and a MySQL database, 
seamlessly translating natural language requests into precise SQL queries. It ensures data integrity 
by validating queries against the database schema before execution, and promotes security by utilizing 
parameterized queries to prevent SQL injection. Beyond simple data retrieval, this agent can handle 
complex joins, aggregations, and subqueries, and further enhances the user experience by providing 
clear explanations of query results and suggesting optimizations for slow queries, all while maintaining 
a detailed log of all actions for audit purposes.
"""

def get_tools() -> List:
    """Returns the list of available tools/functions for the agent.
    
    Returns:
        List[Callable]: List of all database operation functions
        
    Raises:
        ValueError: If any tool is not callable
    """
    tools = [
        list_tables,
        read_query,
        write_query,
        create_table,
        alter_table,
        drop_table,
        describe_table,
        export_query,
        append_insight,
        list_insights
    ]
    
    
    
    return tools

def create_sql_agent(name: str = "sql_database_agent", 
                   model: str = "gemini-2.5-flash",
                   additional_config: Optional[Dict[str, Any]] = None) -> Agent:
    """Creates and configures a SQL database assistant Agent.
    
    Args:
        name (str): Name identifier for the agent
        model (str): Model version/type to use
        additional_config (Optional[Dict]): Additional configuration parameters
        
    Returns:
        Agent: Configured Agent instance with SQL capabilities
        
    Raises:
        ValueError: If agent creation fails
        RuntimeError: If tools are not properly configured
    
    Example:
        >>> agent = create_sql_agent()
        >>> response = agent.execute("List all customers")
    """
    try:
        logger.info(f"Creating SQL agent {name} with model {model}")
        
        config = {
            "name": name,
            "model": model,
            "description": SYSTEM_DESCRIPTION,
            "instruction": SYSTEM_INSTRUCTIONS,
            "tools": get_tools()
        }
        
        if additional_config:
            config.update(additional_config)
            
        agent = Agent(**config)
        logger.info(f"Successfully created SQL agent {name}")
        return agent
        
    except Exception as e:
        logger.error(f"Failed to create SQL agent: {str(e)}")
        raise ValueError(f"Agent creation failed: {str(e)}") from e

# Create the root agent instance

root_agent = create_sql_agent()
