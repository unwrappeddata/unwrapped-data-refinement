from typing import Dict, Any, List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from refiner.models.refined import Base # Assuming Base is correctly defined here
import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

class DataTransformer:
    """
    Base class for transforming JSON data into SQLAlchemy models.
    It initializes the database and provides methods for schema retrieval and saving models.
    """

    def __init__(self, db_path: str):
        """Initialize the transformer with a database path."""
        self.db_path = db_path
        self._initialize_database()

    def _initialize_database(self) -> None:
        """
        Initialize or recreate the database and its tables.
        """
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
                logger.info(f"Deleted existing database at {self.db_path}")
            except OSError as e:
                logger.error(f"Error deleting existing database {self.db_path}: {e}")
                # Depending on severity, you might want to raise this error
                # or attempt to continue if the DB connection can still be made.
                # For now, we'll proceed, and create_engine will handle it.

        try:
            self.engine = create_engine(f'sqlite:///{self.db_path}')
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
            logger.info(f"Database initialized and tables created at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database or create tables at {self.db_path}: {e}")
            raise # Re-raise to halt if DB cannot be set up

    def transform(self, data: Dict[str, Any]) -> List[Base]:
        """
        Transform JSON data into SQLAlchemy model instances.
        Subclasses must implement this method.

        Args:
            data: Dictionary containing the JSON data

        Returns:
            List of SQLAlchemy model instances to be saved to the database
        """
        raise NotImplementedError("Subclasses must implement transform method")

    def get_schema(self) -> str:
        """
        Retrieves the DDL schema for all tables and indexes in the SQLite database.
        """
        if not os.path.exists(self.db_path):
            logger.warning(f"Database file {self.db_path} does not exist. Cannot retrieve schema.")
            return ""

        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            schema_parts = []
            # Get table definitions
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            for table_row in cursor.fetchall():
                if table_row[0]: # Check if sql is not None
                    schema_parts.append(table_row[0] + ";")

            # Get index definitions
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, name") # Order by table then index name
            for index_row in cursor.fetchall():
                if index_row[0]: # Check if sql is not None
                    schema_parts.append(index_row[0] + ";")

            return "\n\n".join(schema_parts)
        except sqlite3.Error as e:
            logger.error(f"SQLite error while getting schema from {self.db_path}: {e}")
            return "" # Return empty string on error
        finally:
            if conn:
                conn.close()

    def save_models(self, models: List[Base]) -> int:
        """
        Saves a list of SQLAlchemy model instances to the database.
        Returns the number of models intended for commit.
        Raises an exception if the commit fails.
        """
        if not models:
            return 0

        session = self.Session()
        try:
            # Add all models to the session.
            # For SQLAlchemy 1.x, session.add_all(models) is okay.
            # For SQLAlchemy 2.x, it's still fine.
            # If there are complex relationships and specific order of operations is needed,
            # individual session.add() calls might be required, but usually add_all handles it.
            session.add_all(models)
            session.commit()
            logger.debug(f"Successfully committed {len(models)} models to the database.")
            return len(models)
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving models to database: {e}")
            # To aid debugging, you could log info about the models that failed
            # for i, m_obj in enumerate(models):
            #     try:
            #         logger.debug(f"Model {i} type {type(m_obj)} content: {m_obj.__dict__}")
            #     except Exception as ie:
            #         logger.debug(f"Could not inspect model {i} type {type(m_obj)}: {ie}")
            raise # Re-raise the exception to be handled by the caller
        finally:
            session.close()