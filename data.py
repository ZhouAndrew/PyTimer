import time
from typing import Any, Dict, List, Optional
import sqlite3

class DataManagerInterface:
    def __init__(self, column_type_dict:dict,database_path:str="data.db"):
        pass
    def get_attr(self,id:int,attr:str)->Any:
        pass
    def set_attr(self,id:int,attr:str)->Any:
        pass
    def add_item(self,attr_dict:dict)->int:# pyright: ignore[reportReturnType] #return id 
        # Don't forget to check type of keys and values in dict!
        pass
    def rm_item(self,id:int):
        pass
    def find_item(self,)->tuple[int]:# pyright: ignore[reportReturnType] #return id 
        pass
    
class DataManager(DataManagerInterface):
    pass