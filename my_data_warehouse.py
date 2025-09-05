from typing import Any, List, Dict
from data_warehouse import DataWarehouse
import csv
import re

class MyDataWarehouse(DataWarehouse):
    """
    Design Overview:
    This warehouse partitions data across multiple CSV files based on 
    the first digit of ZIP codes extracted from addresses.
    
    Partitioning Strategy:
    - Data is distributed across 11 partition files (0.csv through 9.csv, plus x.csv)
    - Partition based on first digit of 5-digit ZIP code in address field
    - Records with invalid/missing ZIP codes go to 'x.csv' partition
    - This achieves roughly even distribution assuming ZIP codes are geographically diverse
    
    Index Design:
    - Maintains a separate index file mapping: id -> partition
    - Index enables O(1) partition lookup for ID-based operations
    - Avoids scanning all partitions for ID queries

    Performance: 
    - Add: O(1) - Simple append to partition and index
    - Query by ID: O(k) where k is number of IDs (uses index)
    - Query by other: O(n) where n is total records (full scan)
    - Update by ID: O(m) where m is partition size
    - Delete by ID: O(m) where m is partition size

    Other stuff we discussed:
    - Cache frequently accessed partitions or index entries to reduce file I/O operations
    """
    
    def __init__(self, file_name: str):
        self.file_name = file_name  # Index file storing id -> partition mapping
        self.fields = ["id", "name", "address", "email"]
        self.index_fields = ["id", "partition"]
        
    def get_zip_first_digit(self, address: str) -> str:
        """Extract first digit of ZIP code from address."""
        # Look for 5-digit ZIP code in address
        zip_match = re.search(r'\b(\d{5})\b', address)
        if zip_match:
            return zip_match.group(1)[0]  # Return first digit of ZIP
        return "x"  # Default partition for invalid ZIP
    
    def add_data(self, data: Dict[str, Any]) -> None:
        # Get partition based on first digit of ZIP code
        partition = self.get_zip_first_digit(data.get("address", ""))
        partition_file = f"{partition}.csv"
        
        # Write full data to partition file
        with open(partition_file, "a", newline="") as f:
            writer = csv.writer(f)
            # Clean newlines to maintain CSV format
            row = [
                data.get("id", ""),
                data.get("name", ""),
                str(data.get("address", "")).replace("\n", "\\n"),
                data.get("email", "")
            ]
            writer.writerow(row)
        
        # Write id -> partition mapping to index file
        with open(self.file_name, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([data.get("id"), partition])
    
    def query_data(self, key_column: str, keys: List[Any]) -> List[Dict[str, Any]]:
        results = []
        keys_set = set(str(k) for k in keys)  # Convert to set for O(1) lookup
        
        if key_column == "id":
            # Use index to find partitions for each ID
            partitions_to_check = {}
            with open(self.file_name, "r", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2 and row[0] in keys_set:
                        id_val = row[0]
                        partition = row[1]
                        if partition not in partitions_to_check:
                            partitions_to_check[partition] = set()
                        partitions_to_check[partition].add(id_val)
            
            # Query each relevant partition
            for partition, ids in partitions_to_check.items():
                partition_file = f"{partition}.csv"
                try:
                    with open(partition_file, "r", newline="") as f:
                        reader = csv.reader(f)
                        for row in reader:
                            if len(row) >= 4 and row[0] in ids:
                                results.append({
                                    "id": row[0],
                                    "name": row[1],
                                    "address": row[2].replace("\\n", "\n"),
                                    "email": row[3]
                                })
                except FileNotFoundError:
                    continue
        else:
            # For non-id queries, check all partition files
            import os
            for file in os.listdir("."):
                if file.endswith(".csv") and file != self.file_name:
                    try:
                        with open(file, "r", newline="") as f:
                            reader = csv.reader(f)
                            for row in reader:
                                if len(row) >= 4:
                                    record = {
                                        "id": row[0],
                                        "name": row[1],
                                        "address": row[2].replace("\\n", "\n"),
                                        "email": row[3]
                                    }
                                    if record.get(key_column) in keys_set:
                                        results.append(record)
                    except FileNotFoundError:
                        continue

        return results
    
    def update_data(self, key_column: str, key_value: Any, updated_data: Dict[str, Any]) -> None:
        key_value = str(key_value)
        
        if key_column == "id":
            # Find partition from index
            old_partition = None
            with open(self.file_name, "r", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2 and row[0] == key_value:
                        old_partition = row[1]
                        break
            
            if old_partition:
                # Update in partition file
                old_file = f"{old_partition}.csv"
                rows = []
                found = False
                try:
                    with open(old_file, "r", newline="") as f:
                        reader = csv.reader(f)
                        for row in reader:
                            if len(row) >= 4 and row[0] == key_value:
                                # Update this row
                                rows.append([
                                    updated_data.get("id", row[0]),
                                    updated_data.get("name", row[1]),
                                    str(updated_data.get("address", row[2])).replace("\n", "\\n"),
                                    updated_data.get("email", row[3])
                                ])
                                found = True
                            else:
                                rows.append(row)
                    
                    if found:
                        with open(old_file, "w", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerows(rows)
                        
                        # Check if partition needs to change
                        new_partition = self.get_zip_first_digit(updated_data.get("address", ""))
                        if new_partition != old_partition:
                            # Move to new partition
                            self.delete_data("id", key_value)
                            self.add_data(updated_data)
                except FileNotFoundError:
                    pass
    
    def delete_data(self, key_column: str, key_value: Any) -> None:
        key_value = str(key_value)
        
        if key_column == "id":
            # Find partition from index
            partition = None
            index_rows = []
            with open(self.file_name, "r", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        if row[0] == key_value:
                            partition = row[1]
                        else:
                            index_rows.append(row)
            
            # Update index file
            with open(self.file_name, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(index_rows)
            
            # Delete from partition file
            if partition:
                partition_file = f"{partition}.csv"
                try:
                    rows = []
                    with open(partition_file, "r", newline="") as f:
                        reader = csv.reader(f)
                        for row in reader:
                            if len(row) >= 4 and row[0] != key_value:
                                rows.append(row)
                    
                    with open(partition_file, "w", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerows(rows)
                except FileNotFoundError:
                    pass
        else:
            # For non-id deletes, check all partition files
            import os
            for file in os.listdir("."):
                if file.endswith(".csv") and file != self.file_name:
                    try:
                        rows = []
                        deleted = False
                        with open(file, "r", newline="") as f:
                            reader = csv.reader(f)
                            for row in reader:
                                if len(row) >= 4:
                                    record = {
                                        "id": row[0],
                                        "name": row[1], 
                                        "address": row[2],
                                        "email": row[3]
                                    }
                                    if record.get(key_column) != key_value:
                                        rows.append(row)
                                    else:
                                        deleted = True
                        
                        if deleted:
                            with open(file, "w", newline="") as f:
                                writer = csv.writer(f)
                                writer.writerows(rows)
                    except FileNotFoundError:
                        continue