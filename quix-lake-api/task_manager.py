import json
import os
import threading
import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, Callable
import queue
import time

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskType(Enum):
    REPARTITION = "repartition"
    COMPACT = "compact"
    DELETE = "delete"
    DISCOVER = "discover"

class Task:
    def __init__(self, task_id: str, task_type: TaskType, params: Dict[str, Any]):
        self.id = task_id
        self.type = task_type
        self.params = params
        self.status = TaskStatus.PENDING
        self.progress = 0
        self.message = "Task created"
        self.created_at = datetime.utcnow().isoformat()
        self.started_at = None
        self.completed_at = None
        self.error = None
        self.result = None
        self.thread = None
        self.pause_flag = threading.Event()
        self.pause_flag.set()  # Not paused by default
        self.cancel_flag = threading.Event()
        self.progress_queue = queue.Queue()
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "params": self.params,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "result": self.result
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        task = cls(
            task_id=data["id"],
            task_type=TaskType(data["type"]),
            params=data["params"]
        )
        task.status = TaskStatus(data["status"])
        task.progress = data["progress"]
        task.message = data["message"]
        task.created_at = data["created_at"]
        task.started_at = data.get("started_at")
        task.completed_at = data.get("completed_at")
        task.error = data.get("error")
        task.result = data.get("result")
        return task

class TaskManager:
    def __init__(self, state_dir: str = "/app/state"):
        self.state_dir = state_dir
        self.tasks_file = os.path.join(state_dir, "tasks.json")
        self.tasks: Dict[str, Task] = {}
        self.executor_callbacks: Dict[TaskType, Callable] = {}
        
        # Ensure state directory exists
        os.makedirs(state_dir, exist_ok=True)
        
        # Load existing tasks
        self._load_tasks()
        
    def _load_tasks(self):
        """Load tasks from persistent storage"""
        if os.path.exists(self.tasks_file):
            try:
                with open(self.tasks_file, 'r') as f:
                    data = json.load(f)
                    for task_data in data:
                        task = Task.from_dict(task_data)
                        # Reset running tasks to paused on restart
                        if task.status == TaskStatus.RUNNING:
                            task.status = TaskStatus.PAUSED
                            task.message = "Task paused due to system restart"
                        self.tasks[task.id] = task
                print(f"Loaded {len(self.tasks)} tasks from state")
            except Exception as e:
                print(f"Error loading tasks: {e}")
                
    def _save_tasks(self):
        """Save tasks to persistent storage"""
        try:
            tasks_data = [task.to_dict() for task in self.tasks.values()]
            with open(self.tasks_file, 'w') as f:
                json.dump(tasks_data, f, indent=2)
        except Exception as e:
            print(f"Error saving tasks: {e}")
            
    def register_executor(self, task_type: TaskType, executor: Callable):
        """Register an executor function for a task type"""
        self.executor_callbacks[task_type] = executor
        
    def create_task(self, task_type: TaskType, params: Dict[str, Any]) -> str:
        """Create a new task"""
        task_id = str(uuid.uuid4())
        task = Task(task_id, task_type, params)
        self.tasks[task_id] = task
        self._save_tasks()
        return task_id
        
    def start_task(self, task_id: str) -> bool:
        """Start or resume a task"""
        task = self.tasks.get(task_id)
        if not task:
            return False
            
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            return False
            
        if task.status == TaskStatus.RUNNING:
            return True  # Already running
            
        # Get executor
        executor = self.executor_callbacks.get(task.type)
        if not executor:
            task.status = TaskStatus.FAILED
            task.error = f"No executor registered for task type {task.type.value}"
            self._save_tasks()
            return False
            
        # Update task status
        task.status = TaskStatus.RUNNING
        if not task.started_at:
            task.started_at = datetime.utcnow().isoformat()
        task.message = "Task started"
        self._save_tasks()
        
        # Create progress callback
        def progress_callback(message: str, progress: int):
            # Check for pause
            task.pause_flag.wait()
            
            # Check for cancellation
            if task.cancel_flag.is_set():
                raise Exception("Task cancelled by user")
                
            # Update progress
            task.progress = progress
            task.message = message
            self._save_tasks()
            
            # Queue progress for streaming
            task.progress_queue.put({
                "progress": progress,
                "message": message,
                "status": task.status.value
            })
        
        # Run task in thread
        def run_task():
            try:
                result = executor(task.params, progress_callback)
                task.status = TaskStatus.COMPLETED
                task.result = result
                task.completed_at = datetime.utcnow().isoformat()
                task.message = "Task completed successfully"
                task.progress = 100
                
                # Queue completion
                task.progress_queue.put({
                    "progress": 100,
                    "message": "Task completed successfully",
                    "status": "completed",
                    "result": result
                })
            except Exception as e:
                task.status = TaskStatus.FAILED
                task.error = str(e)
                task.completed_at = datetime.utcnow().isoformat()
                task.message = f"Task failed: {str(e)}"
                
                # Queue error
                task.progress_queue.put({
                    "progress": task.progress,
                    "message": task.message,
                    "status": "error",
                    "error": str(e)
                })
            finally:
                self._save_tasks()
                
        task.thread = threading.Thread(target=run_task)
        task.thread.daemon = True
        task.thread.start()
        
        return True
        
    def pause_task(self, task_id: str) -> bool:
        """Pause a running task"""
        task = self.tasks.get(task_id)
        if not task or task.status != TaskStatus.RUNNING:
            return False
            
        task.pause_flag.clear()  # Clear the event to pause
        task.status = TaskStatus.PAUSED
        task.message = "Task paused"
        self._save_tasks()
        
        # Queue pause notification
        task.progress_queue.put({
            "progress": task.progress,
            "message": "Task paused",
            "status": "paused"
        })
        
        return True
        
    def resume_task(self, task_id: str) -> bool:
        """Resume a paused task"""
        task = self.tasks.get(task_id)
        if not task or task.status != TaskStatus.PAUSED:
            return False
            
        task.pause_flag.set()  # Set the event to resume
        task.status = TaskStatus.RUNNING
        task.message = "Task resumed"
        self._save_tasks()
        
        # Queue resume notification
        task.progress_queue.put({
            "progress": task.progress,
            "message": "Task resumed",
            "status": "running"
        })
        
        return True
        
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task"""
        task = self.tasks.get(task_id)
        if not task:
            return False
            
        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            return False
            
        task.cancel_flag.set()
        task.pause_flag.set()  # Unpause if paused
        task.status = TaskStatus.CANCELLED
        task.message = "Task cancelled"
        task.completed_at = datetime.utcnow().isoformat()
        self._save_tasks()
        
        # Queue cancellation
        task.progress_queue.put({
            "progress": task.progress,
            "message": "Task cancelled",
            "status": "cancelled"
        })
        
        return True
        
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID"""
        return self.tasks.get(task_id)
        
    def get_all_tasks(self) -> Dict[str, Task]:
        """Get all tasks"""
        return self.tasks
        
    def delete_task(self, task_id: str) -> bool:
        """Delete a completed/failed/cancelled task"""
        task = self.tasks.get(task_id)
        if not task:
            return False
            
        if task.status in [TaskStatus.RUNNING, TaskStatus.PAUSED]:
            return False  # Can't delete active tasks
            
        del self.tasks[task_id]
        self._save_tasks()
        return True
        
    def get_progress_queue(self, task_id: str) -> Optional[queue.Queue]:
        """Get progress queue for a task"""
        task = self.tasks.get(task_id)
        return task.progress_queue if task else None