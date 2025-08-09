
from typing import Any, Dict
from data import DataManager
import time
PAUSED="paused"
RUNNING="running"
class TimerManager:
    
    def __init__(self) -> None:
        self.dm = DataManager(
            column_type_dict={
                "duration":float, # store time in seconds
                "start_time":float,  # timestamp when the timer was started
                "end_time":float,    # timestamp when the timer was ended
                "status":str,        # status of the timer (e.g., "running", "paused", "stopped")
                "name":str,           # name of the timer
            }
                ,database_path="data.db"
        )
    # def is_timer_exists(self, timer_id: int) -> bool:
    #     if not isinstance(timer_id, int) or timer_id <= 0:
    #         raise ValueError("Timer ID must be a positive integer.")
    #     timer=self.dm.find_item(self,)

    def get_timer_attr(self, timer_id: int, attr: str) -> Any:
        if not isinstance(timer_id, int) or timer_id <= 0:
            raise ValueError("Timer ID must be a positive integer.")
        if not self.is_timer_exists(timer_id):
            raise ValueError("Timer with this ID does not exist.")
        return self.dm.get_attr(timer_id, attr)
    def create_timer(self, name: str, duration: int) -> int:
        if not isinstance(name, str) or not isinstance(duration, int):
            raise ValueError("Name must be a string and duration must be an integer.")
        if duration <= 0:
            raise ValueError("Duration must be a positive integer.")
        if not name:
            raise ValueError("Name cannot be empty.")
        if len(name) > 100:
            raise ValueError("Name cannot exceed 100 characters.")
        this_moment=time.time()
        
        return self.dm.add_item({
            "name": name,
            "duration": duration,
            "start_time":this_moment,
            "end_time": this_moment+duration,
            "status": PAUSED
        })
    def rm_timer(self, timer_id: int) -> None:
        if not isinstance(timer_id, int) or timer_id <= 0:
            raise ValueError("Timer ID must be a positive integer.")
        if not self.dm.get_attr(timer_id, "name"):
            raise ValueError("Timer with this ID does not exist.")
        self.dm.rm_item(timer_id)
    def pause_timer(self, timer_id: int) -> None: