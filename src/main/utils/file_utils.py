from typing import Union, List, Any, Dict
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import pandas as pd
import csv, json
import random


class FileUtils():
    @staticmethod
    def read_from_parquet_as_table(file_path: str, schema: Any | None = None):
        """
        Read the content of a file.

        Parameters:
        - file_path (str): The path to the file.

        Returns:
        - table (from parquet): The content of the file.
        """
        table = pq.read_table(file_path, schema=schema)
        # for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
        #     #TODO: group batches if necessary
        #     break
        return table
    
    @staticmethod
    def subset_parquet_file(input_file_path: List[str], output_file_path: str, num_rows: int = -1, random_sample = False):
        """Write only a specified number of rows from a parquet file to output parquet file

        Args:
            input_file_path (List[str]): a parquet file path or list of parquet file path
            output_file_path (str): an output parquet file path
            num_rows (int, optional): number of records to output. Defaults to -1 (all records).
            random (boolean): whether to sample randomly the number of records to output
        """        
        list_data = list(); field_names = list(); total_rows = 0
        for path in input_file_path:
            table = FileUtils.read_from_parquet_as_table(path)
            field_names.extend(table.column_names)
            list_data.extend(table.to_pylist())
            total_rows = len(list_data)
            if  total_rows> num_rows and num_rows != -1:
                break
        if random_sample:
            random_list_data = random.sample(list_data, num_rows)
            if num_rows != -1:
                df = pd.DataFrame(random_list_data[:num_rows], columns=field_names)
            else:
                df = pd.DataFrame(random_list_data[:], columns=field_names)
        else:
            if num_rows != -1:
                df = pd.DataFrame(list_data[:num_rows], columns=field_names)
            else:
                df = pd.DataFrame(list_data[:], columns=field_names)
        df.to_parquet(output_file_path)
        print(f"Total records: {total_rows}")
        print(f"Requested records: {num_rows if num_rows != -1 else total_rows}")
    
    @staticmethod
    def convert_parquet_to_csv(input_file_path: List[str], output_file_path: str, num_rows: int = -1):
        """Convert parquet file(s) to a csv file

        Args:
            input_file_path (List[str]): a file path or list of file path
            output_file_path (str): an output file path
            num_rows (int, optional): number of records to output. Defaults to -1 (all records).
        """        
        list_data = list(); field_names = list(); total_rows = 0
        for path in input_file_path:
            table = FileUtils.read_from_parquet_as_table(path)
            field_names.extend(table.column_names)
            list_data.extend(table.to_pylist())
            total_rows = len(list_data)
            if  total_rows> num_rows and num_rows != -1:
                break
        with open(output_file_path, 'w', newline='') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=field_names)
            csv_writer.writeheader()
            if num_rows == -1:
                csv_writer.writerows(list_data)
            else:
                for idx in range(num_rows):
                    csv_writer.writerow(list_data[idx])
        print(f"Total records: {total_rows}")
        print(f"Requested records: {num_rows if num_rows != -1 else total_rows}")

    @staticmethod
    def merge_parquet_files(input_file_paths: List[str], output_file_path: str) -> None:
        schema = pq.ParquetFile(input_file_paths[0]).schema_arrow
        with pq.ParquetWriter(output_file_path, schema) as writer:
            for file_path in input_file_paths:
                writer.write_table(FileUtils.read_from_parquet_as_table(file_path, schema=schema))

    @staticmethod
    def convert_excel_to_parquet(filepath: str, sheet_name: Union[int, str] = 0, skiprows = None, mapping: Dict = None, to_csv: bool = False):
        """Converts an input Excel file to parquet file and csv file.

        Args:
            filepath (str): The path to the file.
            sheet_name (Union[int, str], optional): Name or Index of the sheets in excel file.. Defaults to 0 (first sheet).
            mapping (Dict, optional): A dictionary consists of excel field names as keys and expected field names in parquet to be compliant with QUERY_FIELD, GROUND_TRUTH_FIELD and REFERENCE_RESPONSE_FIELD from config/constants.py. Defaults to None.
            to_csv (bool, optional): Whether to write to csv or not. Defaults to False.
        """            
        df = pd.read_excel(filepath, sheet_name=sheet_name, skiprows=skiprows)
        df.dropna(subset=list(mapping.keys()) if mapping else None, inplace=True)
        if mapping:
            df_new = pd.DataFrame()
            for old_field, new_field in mapping.items():
                df_new[new_field] = df[old_field]
            list_metadata_json = df_new["metadata_json"].to_list()
            list_metadata_json_new = list()
            if list_metadata_json:
                for metadata_json in list_metadata_json:
                    if isinstance(metadata_json, str):
                        list_metadata_json_new.append(json.dumps({"url": list(filter(lambda value: value, metadata_json.splitlines()))}))
            df_new["metadata_json"] = list_metadata_json_new
            if to_csv:
                df_new.to_csv(filepath.replace(".xlsx", ".csv"))
            df_new.to_parquet(filepath.replace(".xlsx", ".parquet"))
        else:
            if to_csv:
                df.to_csv(filepath.replace(".xlsx", ".csv"))
            df.to_parquet(filepath.replace(".xlsx", ".parquet"))

    @staticmethod
    def convert_csv_to_parquet(input_file_path: str, output_file_path: str):
        df = pd.read_csv(input_file_path)
        print(f"Total records and columns: {df.shape}")
        df.to_parquet(output_file_path)

    @staticmethod
    def convert_arrow_to_csv(input_file_path: str, output_file_path: str):
        with ipc.open_file(input_file_path) as reader:
            table = reader.read_all()
        # Convert the table to a pandas DataFrame
        df = table.to_pandas()
        print(f"Total records and columns: {df.shape}")
        df.to_csv(output_file_path)
        



