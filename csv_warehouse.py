from typing import Any, List, Dict
from data_warehouse import DataWarehouse
import csv

class NaiveCSVWarehouse(DataWarehouse):
    """
    Naive C
V Implentation    """
    
    
    def __init__(self, file_path: str):
        self.file_path = file_path

    def add_data(self, data: Dict[str, Any]) -> None:
        # Implementation here
        with open(self.file_path, "a+") as csv_file:
            csv_file.write(",".join([str(value) for value in data.values()]))
        raise NotImplementedError("This method is not implemented yet.")

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        # Implementation here
        rows = []
        with open(self.file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get(key_column) == str(key_value):
                    row.update(updated_data)
                rows.append(row)
        
        with open(self.file_path, 'w', newline='') as csvfile:
            if rows:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
       
    def delete_data(self, key_column: str, key_value: Any) -> None:
        # Implementation here
        with open(self.file_path, 'r') as csv_file:
            cur_data_list = [row for row in csv.DictReader(csv_file)]
        
        for i, row in enumerate(cur_data_list):
            if row.get(key_column) == key_value:
                del cur_data_list[i]
                break
        
        #update by overwriting using new list, not very efficient
        with open(self.file_path, 'w', newline='') as csv_file:
            csv.DictWriter(csv_file, fieldnames=cur_data_list[0].keys()).writeheader()
            print("data deleted successfully")
        
            
            
        
            
    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        # Implementation here

        results = []
        str_keys = [str(key) for key in keys]
        with open(self.file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get(key_column) in str_keys:
                    results.append(row)
        return results