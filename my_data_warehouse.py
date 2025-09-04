from typing import Any, List, Dict
from data_warehouse import DataWarehouse
import csv
import re

class MyDataWarehouse(DataWarehouse):
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.fields = ["id", "name", "state", "address", "email"]

    # def _extract_state(self, address: str) -> str:
    #     state_pattern = re.compile(r',\s*([A-Z]{2})\s+(\d{5})')
    #     match = state_pattern.search(address)
    #     if match:
    #         return match.group(2)
    #     return ""

    def add_data(self, data: Dict[str, Any]) -> None:
        # if not self.fields:
        #     self.fields = list(data.keys())

        with open(self.file_name, "a") as csv_warehouse:
            writer = csv.writer(csv_warehouse)
            # replace new line in data values with '\n' to keep csv format intact
            converted_values = [str(value).replace("\n", "\\n") for value in data.values()]


            converted_values = []
            state = "";
            i = 0
            id1 = "";
            zip = "";
            for value in data.values():
                if i == 0:
                    id1 = str(value)

                if i == 2:
                    tmp = str(value).replace("\n", "\\n")
                    # print(tmp[-5])
                    converted_values.append(tmp[-5])
                    state = tmp[-5]
                    
                    converted_values.append(tmp)
                else:
                    converted_values.append(value)
                i += 1

                filename = state+".csv"
            with open(filename, "a") as state_csv:
                state_writer = csv.writer(state_csv)
                state_writer.writerow(converted_values)





            meta = [ id1, state ]
            writer.writerow(meta)

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
                        # print(row['name'])
                        name = row['name'] + ".csv"
                        with open(name, "r") as state_csv:
                            state_reader = csv.DictReader(state_csv, fieldnames=self.fields)
                            for state_row in state_reader:
                                if state_row[key_column] == key:
                                    rows.append(state_row)
        return rows
