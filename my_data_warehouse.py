from typing import Any, List, Dict
from data_warehouse import DataWarehouse


class MyDataWarehouse(DataWarehouse):
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.fields = None

    def add_data(self, data: Dict[str, Any]) -> None:
        if not self.fields:
            self.fields = list(data.keys())
        with open(self.file_name, "a") as csv_warehouse:
            writer = csv.writer(csv_warehouse)
            # replace new line in data values with '\n' to keep csv format intact
            converted_values = [str(value).replace("\n", "\\n") for value in data.values()]
            writer.writerow(converted_values)

    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        rows = []
        with open(self.file_name, "r") as csv_warehouse:
            reader = csv.DictReader(csv_warehouse, fieldnames=self.fields)
            for row in reader:
                if row[key_column] == key_value:
                    rows.append(updated_data)
                else:
                    rows.append(row)

        with open(self.file_name, "w") as csv_warehouse:
            writer = csv.DictWriter(csv_warehouse, fieldnames=self.fields)
            writer.writerows(rows)
            
    def delete_data(self, key_column: str, key_value: Any) -> None:
        rows = []
        with open(self.file_name, "r") as csv_warehouse:
            reader = csv.DictReader(csv_warehouse, fieldnames=self.fields)
            for row in reader:
                if row[key_column] == key_value:
                    continue
                else:
                    rows.append(row.values())

        with open(self.file_name, "w") as csv_warehouse:
            writer = csv.writer(csv_warehouse)
            writer.writerows(rows)

    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        rows = []
        for key in keys:
            with open(self.file_name, "r", newline="") as csv_warehouse:
                reader = csv.DictReader(csv_warehouse, fieldnames=self.fields)
                for row in reader:
                    if row[key_column] == key:
                        rows.append(row)
        return rows
