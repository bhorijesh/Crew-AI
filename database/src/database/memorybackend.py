import os
from dotenv import load_dotenv

class MySQLMemoryBackend:
    def __init__(self, db_connection):
        """
        Initialize the MySQLMemoryBackend with a DbConnection instance.
        :param db_connection: An instance of DbConnection.
        """
        self.db_connection = db_connection
        self.connection = None
        self.cursor = None
        self._ensure_table_exists()

    def _ensure_connection(self):
        """Ensure that the raw connection and cursor are initialized."""
        try:
            # Check if the connection is None
            if self.connection is None:
                self.connection = self.db_connection.engine.raw_connection()
                self.cursor = self.connection.cursor()
                print("Database connection and cursor initialized.")
        except Exception as e:
            print(f"Failed to ensure connection: {str(e)}")
            raise

    def _ensure_table_exists(self):
        """Ensure that the 'crew_memory' table exists."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS crew_memory (
            id INT AUTO_INCREMENT PRIMARY KEY,
            `key` VARCHAR(255) NOT NULL,
            value TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            self._ensure_connection()
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            print("Table 'crew_memory' ensured to exist.")
        except Exception as e:
            print(f"Failed to ensure table exists: {str(e)}")
            raise

    def store(self, key, value):
        """Store a key-value pair in the database."""
        query = "INSERT INTO crew_memory (`key`, value) VALUES (%s, %s)"  
        try:
            self._ensure_connection()
            self.cursor.execute(query, (key, value))
            self.connection.commit()
            print(f"Data stored successfully: key={key}, value={value}")
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            print(f"Failed to store data: {str(e)}")
            raise

    def retrieve(self, key):
        """Retrieve the latest value for a given key."""
        query = "SELECT value FROM crew_memory WHERE `key` = %s ORDER BY created_at DESC LIMIT 1"  
        try:
            self._ensure_connection()
            self.cursor.execute(query, (key,))
            result = self.cursor.fetchone()
            print(f"Data retrieved successfully: key={key}, value={result[0] if result else None}")
            return result[0] if result else None
        except Exception as e:
            print(f"Failed to retrieve data: {str(e)}")
            raise

    def close(self):
        """Close the cursor and connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print("MySQLMemoryBackend connection closed successfully.")
        except Exception as e:
            print(f"Failed to close MySQLMemoryBackend connection: {str(e)}")
            raise