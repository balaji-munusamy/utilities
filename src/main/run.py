from utils.file_utils import FileUtils
from utils.visual_utils import VisualUtils
from utils.file_utils import FileUtils

if __name__ == "__main__":
    # Convert parquet to csv
    print("process has begun!")
    test_file_path = ["some.parquet"]
    # FileUtils.convert_parquet_to_csv(test_file_path, test_file_path[0].replace(".parquet", ".csv"))
    FileUtils.convert_parquet_to_csv(test_file_path, "data/some.csv", 5)
    print("completed!")

    # # Write only specific number of records into a parquet file
    # print("process has begun!")
    # test_file_path = ["data/some.parquet"]
    # FileUtils.subset_parquet_file(test_file_path, "data/some.parquet", 5, False)
    # print("completed!")

    #Draw line chart
    # VisualUtils.draw_line_chart_from_csv("data/runs.csv", x_field="Name",\
    #                                          y_fields=["Google_MRR", "Chat_MRR", "Chat_BLEU", "Chat_ROUGE"])
    
    # Merge parquet files
    # list_files = [
    #     "some.parquet",
    #     "some.parquet",
    # ]
    # output_file = "some_response.parquet"
    # FileUtils.merge_parquet_files(list_files, output_file)

    #Convert xlsx to parquet file
    # FileUtils.convert_excel_to_parquet("data/Questions annotated from April 2024.xlsx", \
    #                                    mapping={"prompt": "instruction", "Answer": "reference_response", "Context": "metadata_json"},
    #                                    to_csv=True)

    #Convert csv to parquert file
    # FileUtils.convert_csv_to_parquet("some.csv", "data/some.parquet")

    # Convert arrow to csv
    # FileUtils.convert_arrow_to_csv("data/data-00000-of-00001.arrow",
    #                                "data/data-00000-of-00001.csv")
    # import pandas as pd
    # import pyarrow.ipc as ipc
    # import pyarrow

    # # Use the path to the Arrow file
    # file_path = 'data/data-00000-of-00001.arrow'

    # try:
    #     # Open and read the Arrow file
    #     with ipc.open_file(file_path) as reader:
    #         table = reader.read_all()
        
    #     # Convert the table to a pandas DataFrame
    #     df = table.to_pandas()
        
    #     # Display the DataFrame
    #     print(df)
    # except pyarrow.lib.ArrowInvalid as e:
    #     print(f"Error: {e}")
    #     print("The file is not a valid Arrow file.")
    # except FileNotFoundError as e:
    #     print(f"Error: {e}")
    #     print("Please check the file path and ensure the file exists.")

