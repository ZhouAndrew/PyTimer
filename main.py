
from typing import Any, Dict
from data import DataManager
PAUSED="paused"

class TimerManager:
    
    def __init__(self) -> None:
        self.dm = DataManager()        
        
        
    def create_timer(self, name: str, duration: int) -> int:
        if not isinstance(name, str) or not isinstance(duration, int):
            raise ValueError("Name must be a string and duration must be an integer.")
        if duration <= 0:
            raise ValueError("Duration must be a positive integer.")
        if not name:
            raise ValueError("Name cannot be empty.")
        if len(name) > 100:
            raise ValueError("Name cannot exceed 100 characters.")
        pass