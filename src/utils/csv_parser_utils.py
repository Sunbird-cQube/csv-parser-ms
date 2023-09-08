import typing
import pandas as pd
import os
from datetime import datetime
import json
import re, shutil
from .utils import create_folder_if_not, get_directory_structure
from ..settings import DEFAULT_PROGRAM_NAME, TMP_BASE_PATH


def format_df_columns(df: pd.DataFrame):
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

    old_column_mapping = {col: clean_column_name(col) for col in df.columns}
    df.columns = [clean_column_name(col) for col in df.columns]
    return old_column_mapping


def guess_metrics_and_columns(token: str, filename: str):
    file_path = os.path.join(TMP_BASE_PATH, token, filename)
    df = pd.read_csv(file_path)
    column_type_dict = dict()

    for column in df.columns:
        if df[column].dtype == "int":
            column_type_dict[df[column].name] = {"metric": True, "dimension": False}
        else:
            column_type_dict[df[column].name] = {"metric": False, "dimension": True}
    return column_type_dict


def generate_ingest_files(token: str, column_metadata: typing.Dict):
    folder_path = os.path.join(TMP_BASE_PATH, token)
    file_path = os.path.join(
        folder_path,
        [file for file in os.listdir(folder_path) if file.endswith(".csv")][0],
    )
    df = pd.read_csv(file_path)

    column_mapping = format_df_columns(df)

    metrics, dimensions = [], []

    for cols in column_metadata:
        if column_metadata[cols]["metric"]:
            metrics.append(column_mapping[cols])
        else:
            dimensions.append(column_mapping[cols])

    ingest_folder_path = os.path.join(folder_path, "ingest")

    default_program_name = os.getenv("DEFAULT_PROGRAM_NAME")
    default_program_desc = default_program_name + " desc"
    write_dimensions_to_ingest_folder(df, dimensions, ingest_folder_path)
    write_events_to_ingest_folder(
        df, dimensions, metrics, default_program_name, ingest_folder_path
    )
    write_config_to_ingest_folder(
        default_program_name, default_program_desc, ingest_folder_path
    )

    return {"dimension": dimensions, "metrics": metrics}


def write_dimensions_to_ingest_folder(
    df: pd.DataFrame, dimensions: typing.Iterable, ingest_folder_path: str
):
    dimensions_base_path = os.path.join(ingest_folder_path, "dimensions")
    create_folder_if_not(dimensions_base_path)

    print(
        ":boom: [blue]Generating Dimension Grammar and Dimension Data[/blue] from CSV"
    )
    for dimension in dimensions:
        dimension_grammar_data = f"""PK,Index
string,string
{dimension}_id,{dimension}"""
        with open(
            os.path.join(dimensions_base_path, f"{dimension}-dimension.grammar.csv"),
            "w",
        ) as f:
            f.write(dimension_grammar_data)
        column_df = pd.DataFrame(df[dimension].drop_duplicates(keep="first"))
        column_df.insert(
            loc=0, column=f"{dimension}_id", value=range(1, len(column_df) + 1)
        )
        column_df.to_csv(
            os.path.join(dimensions_base_path, f"{dimension}-dimension.data.csv"),
            index=False,
        )


def write_events_to_ingest_folder(
    df: pd.DataFrame, dimensions, metrics, program_name, ingest_folder_path
):
    print(":boom: [blue]Generating Event Grammar and Event Data[/blue] from CSV")
    events_base_path = os.path.join(ingest_folder_path, "programs", program_name)
    create_folder_if_not(events_base_path)

    for metric in metrics:
        with open(
            os.path.join(events_base_path, f"{metric}-event.grammar.csv"), "w"
        ) as f:
            f.write("," + ",".join(dimensions) + "," + "\n")
            f.write("," + ",".join(dimensions) + "," + "\n")
            f.write("date," + "string," * (len(dimensions)) + "integer" "\n")
            f.write("date," + ",".join(dimensions) + f",{metric}" + "\n")
            f.write(
                "timeDimension," + "dimension," * (len(dimensions)) + "metric" + "\n"
            )
        headers = dimensions + [metric]
        event_df = pd.DataFrame(df[headers])
        event_df.insert(
            loc=0, column=f"date", value=datetime.today().strftime("%d/%m/%y")
        )
        event_df.to_csv(
            os.path.join(events_base_path, f"{metric}-event.data.csv"), index=False
        )


def write_config_to_ingest_folder(
    program_name, program_description, ingest_folder_path
):
    print(":boom: [blue]Generating Config File [/blue] from CSV")

    config_template = {
        "globals": {"onlyCreateWhitelisted": "true"},
        "dimensions": {
            "namespace": "dimensions",
            "fileNameFormat": "${dimensionName}.${index}.dimensions.data.csv",
            "input": {"files": "./ingest/dimensions"},
        },
        "programs": [
            {
                "name": program_name,
                "namespace": program_name,
                "description": program_description,
                "shouldIngestToDB": "true",
                "input": {"files": f"./ingest/programs/{program_name}"},
                "./output": {"location": f"./output/programs/{program_name}"},
                "dimensions": {"whitelisted": [], "blacklisted": []},
            }
        ],
    }

    config_json = json.dumps(config_template, indent=4)

    with open(os.path.join(ingest_folder_path, "config.json"), "w") as f:
        f.write(config_json)


def get_dimensions(token: str):
    folder_path = os.path.join(TMP_BASE_PATH, token, "ingest/", "dimensions")
    return get_directory_structure(folder_path)


def get_events(token: str):
    folder_path = os.path.join(
        TMP_BASE_PATH, token, "ingest/programs", DEFAULT_PROGRAM_NAME
    )
    return get_directory_structure(folder_path)

def download_ingest_folder(token: str):
    ingest_folder_path = os.path.join(TMP_BASE_PATH, token, 'ingest')
    zip_location_path = os.path.join(TMP_BASE_PATH, token, 'cqube-ingest')
    return shutil.make_archive(zip_location_path, 'zip', ingest_folder_path)
