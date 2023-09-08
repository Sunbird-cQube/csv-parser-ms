import typing
import pandas as pd
import os
from datetime import datetime
import json
import re
from fastapi import UploadFile


def format_df_columns(filename, token):
    base_path = os.getenv("TMP_BASE_PATH")
    file_path = os.path.join(base_path, token, filename)
    df = pd.read_csv(file_path)
    unformatted_column_names = df.columns.to_list()

    def clean_column_name(column_name):
        # Convert to lowercase
        column_name = column_name.lower()
        # Remove special characters (replace with '')
        column_name = re.sub(r"[^a-zA-Z0-9]", " ", column_name)
        column_name = column_name.strip()
        column_name = re.sub(r" {2,}", " ", column_name)
        # Replace spaces with underscores
        column_name = column_name.replace(" ", "_")
        return column_name

    df.columns = [clean_column_name(col) for col in df.columns]
    return unformatted_column_names, df.columns.to_list()


# def get_metrics_and_columns(df: pd.DataFrame):
#     metrics = []
#     dimensions = []
#
#     for column in df.columns:
#         if df[column].dtype == "int":
#             res = typer.confirm(f'Do you want "{column}" to be a metric', True)
#             if res:
#                 metrics.append(column)
#             else:
#                 dimensions.append(column)
#         else:
#             dimensions.append(column)
#     return dimensions, metrics


# def write_dimensions_to_ingest_folder(
#     df: pd.DataFrame, dimensions: typing.Iterable, ingest_folder_path: str
# ):
#     dimensions_base_path = os.path.join(ingest_folder_path, "dimensions")
#     create_folder_if_not(dimensions_base_path)
#
#     print(
#         ":boom: [blue]Generating Dimension Grammar and Dimension Data[/blue] from CSV"
#     )
#     for dimension in dimensions:
#         dimension_grammar_data = f"""PK,Index
# string,string
# {dimension}_id,{dimension}"""
#         with open(
#             os.path.join(dimensions_base_path, f"{dimension}-dimension.grammar.csv"),
#             "w",
#         ) as f:
#             f.write(dimension_grammar_data)
#         column_df = pd.DataFrame(df[dimension].drop_duplicates(keep="first"))
#         column_df.insert(
#             loc=0, column=f"{dimension}_id", value=range(1, len(column_df) + 1)
#         )
#         column_df.to_csv(
#             os.path.join(dimensions_base_path, f"{dimension}-dimension.data.csv"),
#             index=False,
#         )


# def write_events_to_ingest_folder(
#     df: pd.DataFrame, dimensions, metrics, program_name, ingest_folder_path
# ):
#     print(":boom: [blue]Generating Event Grammar and Event Data[/blue] from CSV")
#     events_base_path = os.path.join(ingest_folder_path, "programs", program_name)
#     create_folder_if_not(events_base_path)
#
#     for metric in metrics:
#         with open(
#             os.path.join(events_base_path, f"{metric}-event.grammar.csv"), "w"
#         ) as f:
#             f.write("," + ",".join(dimensions) + "," + "\n")
#             f.write("," + ",".join(dimensions) + "," + "\n")
#             f.write("date," + "string," * (len(dimensions)) + "integer" "\n")
#             f.write("date," + ",".join(dimensions) + f",{metric}" + "\n")
#             f.write(
#                 "timeDimension," + "dimension," * (len(dimensions)) + "metric" + "\n"
#             )
#         headers = dimensions + [metric]
#         event_df = pd.DataFrame(df[headers])
#         event_df.insert(
#             loc=0, column=f"date", value=datetime.today().strftime("%d/%m/%y")
#         )
#         event_df.to_csv(
#             os.path.join(events_base_path, f"{metric}-event.data.csv"), index=False
#         )
#
#
# def write_config_to_ingest_folder(
#     program_name, program_description, ingest_folder_path
# ):
#     print(":boom: [blue]Generating Config File [/blue] from CSV")
#
#     config_template = {
#         "globals": {"onlyCreateWhitelisted": "true"},
#         "dimensions": {
#             "namespace": "dimensions",
#             "fileNameFormat": "${dimensionName}.${index}.dimensions.data.csv",
#             "input": {"files": "./ingest/dimensions"},
#         },
#         "programs": [
#             {
#                 "name": program_name,
#                 "namespace": program_name,
#                 "description": program_description,
#                 "shouldIngestToDB": "true",
#                 "input": {"files": f"./ingest/programs/{program_name}"},
#                 "./output": {"location": f"./output/programs/{program_name}"},
#                 "dimensions": {"whitelisted": [], "blacklisted": []},
#             }
#         ],
#     }
#
#     config_json = json.dumps(config_template, indent=4)
#
#     with open(os.path.join(ingest_folder_path, "config.json"), "w") as f:
#         f.write(config_json)
#
#
# def create_ingest_folder(raw_csv_file_path: str, ingest_folder_path: str):
#     print(f"Processing CSV file: {raw_csv_file_path}")
#
#     try:
#         df = pd.read_csv(raw_csv_file_path)
#     except Exception as e:
#         print("An error occured", e)
#         exit(1)
#
#     df = format_df_columns(df)
#
#     dimensions, metrics = get_metrics_and_columns(df)
#     print("Dimensions are", dimensions)
#     print("Metrics are", metrics)
#
#     write_dimensions_to_ingest_folder(df, dimensions, ingest_folder_path)
#
#     program_name = typer.prompt("Input the Program Name")
#     program_description = typer.prompt("Input the Program Descrption")
#
#     write_events_to_ingest_folder(
#         df, dimensions, metrics, program_name, ingest_folder_path
#     )
#     write_config_to_ingest_folder(program_name, program_description, ingest_folder_path)


def create_folder_if_not(path: str):
    if not os.path.exists(path):
        os.makedirs(path)
