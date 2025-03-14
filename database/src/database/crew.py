from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task
from dotenv import load_dotenv
from connect_db import DbConnection
from memorybackend import MySQLMemoryBackend

load_dotenv()

@CrewBase
class Database():
    """Database crew"""

    agents_config = 'config/agents.yaml'
    tasks_config = 'config/tasks.yaml'

    def __init__(self):
        """
        Initialize the Database crew with a shared memory backend.
        """
        # Initialize DbConnection
        db_connection = DbConnection()
        db_connection.get_server_connection()
        self.memory_backend = MySQLMemoryBackend(db_connection)

    @agent
    def researcher(self) -> Agent:
        return Agent(
            config=self.agents_config['researcher'],
            verbose=True,
            memory=self.memory_backend  
        )

    @agent
    def reporting_analyst(self) -> Agent:
        return Agent(
            config=self.agents_config['reporting_analyst'],
            verbose=True,
            memory=self.memory_backend  
        )

    @task
    def research_task(self) -> Task:
        print("Creating research task...")
        return Task(
            config=self.tasks_config['research_task'],  
            agent=self.researcher(),
            context=[
                {
                    "memory": self.memory_backend,
                    "description": "Shared memory context for the research task.",
                    "expected_output": "Data retrieved from shared memory."
                }
            ],
            async_run=False,  # Ensure the task runs synchronously for debugging
            callback=self._store_task_output  
        )

    @task
    def reporting_task(self) -> Task:
        print("Creating reporting task...")
        return Task(
            config=self.tasks_config['reporting_task'],  
            agent=self.reporting_analyst(),
            output_file='report.md',
            context=[
                {
                    "memory": self.memory_backend,
                    "description": "Shared memory context for the reporting task.",
                    "expected_output": "Data retrieved from shared memory."
                }
            ],
            async_run=False,  
            callback=self._store_task_output  
        )

    def _store_task_output(self, task_output):
        """
        Store the task output in the database using the memory backend.
        """
        # Generate a unique key based on the task description (or use a generic key)
        key = f"task_output_{hash(task_output.description)}"  # Use hash of task description for uniqueness

        try:
            value = task_output.raw  # The actual output of the task is stored in the 'raw' attribute
            self.memory_backend.store(key, value)
            print(f"Task output stored successfully: key={key}")  
        except Exception as e:
            print(f"Failed to store task output: {str(e)}")

    @crew
    def crew(self) -> Crew:
        """Creates the Database crew"""
        return Crew(
            agents=self.agents,  # Automatically created by the @agent decorator
            tasks=self.tasks,  # Automatically created by the @task decorator
            process=Process.sequential,
            verbose=True,
        )