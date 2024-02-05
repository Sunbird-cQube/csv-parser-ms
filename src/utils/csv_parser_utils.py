import typing
import pandas as pd
import os
from datetime import datetime
import json
import re, shutil
from .utils import create_folder_if_not, get_directory_structure
from ..settings import TMP_BASE_PATH


def format_df_columns(df: pd.DataFrame, column_metadata):
    # update column names with user specified column names

    user_specified_column_name = {
        old_column: mapping["updated_col_name"]
        for old_column, mapping in column_metadata.items()
    }

    df = df.rename(columns=user_specified_column_name)

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
    return df, old_column_mapping


def guess_metrics_and_columns(token: str, filename: str):
    file_path = os.path.join(TMP_BASE_PATH, token, filename)
    df = pd.read_csv(file_path)
    column_type_dict = dict()

    for column in df.columns:
        column_type_dict[df[column].name] = {
            "updated_col_name": df[column].name,
            "type": str(df[column].dtype),
            "metric": False,
            "timeDimension": False,
            "dimension": False
        }
    return column_type_dict


def generate_ingest_files(
    token: str, column_metadata: typing.Dict, program_name: str, program_desc: str, dimensions: typing.Iterable
):
    folder_path = os.path.join(TMP_BASE_PATH, token)
    file_path = os.path.join(
        folder_path,
        [file for file in os.listdir(folder_path) if file.endswith(".csv")][0],
    )
    df = pd.read_csv(file_path)

    df, column_mapping = format_df_columns(df, column_metadata)
    metrics, otherCols, dimensionFkCols = [], [], []

    for cols in column_metadata:
        updated_col_name = column_metadata[cols]["updated_col_name"]
        if column_metadata[cols]["metric"]:
            metrics.append(column_mapping[updated_col_name])
        elif column_metadata[cols]["dimension"]:
            print('Do Nothing')
        else:
            otherCols.append(cols)
    
    for dimension in dimensions:
        if (len(dimension.fields) > 1):
            for field in dimension.fields:
                if (field.isIndex):
                    data = { "key": field.data.key, "name": dimension.name }
                    dimensionFkCols.append(data)
                    break
        else:
            data = { "key": f"{dimension.name}_id", "name": dimension.name }
            dimensionFkCols.append(data)

    ingest_folder_path = os.path.join(folder_path, "ingest")

    write_dimensions_to_ingest_folder(df, dimensions, column_metadata, ingest_folder_path)
    write_config_to_ingest_folder(program_name, program_desc, ingest_folder_path)
    write_events_to_ingest_folder(program_name, df, metrics, dimensions, ingest_folder_path, dimensionFkCols, otherCols, column_metadata)

    return {"dimension": dimensions, "metrics": metrics}

def getType(dataType: str):
    if (dataType == 'int64' or dataType == 'float64'):
        return 'number'
    elif (dataType == 'datetime64'):
        return 'date'
    elif (dataType == 'object'):
        return 'string'
    else:
        return 'string'

def write_dimensions_to_ingest_folder(
    df: pd.DataFrame, dimensions: typing.Iterable, column_metadata: typing.Dict, ingest_folder_path: str
):
    dimensions_base_path = os.path.join(ingest_folder_path, "dimensions")
    create_folder_if_not(dimensions_base_path)

    for dimension in dimensions:
        index_row, type_row, column_row, indexed_col, req_cols  = [], [], [], [], []
        if (len(dimension.fields) > 1):
            for field in dimension.fields:
                if (field.isPrimary):
                    index_row.append('PK')
                    indexed_col.append(column_metadata[field.value]['updated_col_name'])
                elif (field.isIndex):
                    index_row.append('Index')
                    indexed_col.append(column_metadata[field.value]['updated_col_name'])
                else:
                    index_row.append('')
            
                type_row.append(getType(column_metadata[field.value]['type']))
                column_row.append(column_metadata[field.value]['updated_col_name'])
                req_cols.append(column_metadata[field.value]['updated_col_name'])
        else:
            index_row.append('PK')
            type_row.append('number')
            column_row.append(f"{dimension.name}_id")
            
            index_row.append('Index')
            type_row.append(getType(column_metadata[dimension.fields[0].value]['type']))
            column_row.append(column_metadata[dimension.fields[0].value]['updated_col_name'])

            indexed_col.append(column_metadata[dimension.fields[0].value]['updated_col_name'])
            req_cols.append(column_metadata[dimension.fields[0].value]['updated_col_name'])
        dimension_grammar_data = f"""{','.join(index_row)}
{','.join(type_row)}
{','.join(column_row)}
"""
            
        with open(
            os.path.join(dimensions_base_path, f"{dimension.name}-dimension.grammar.csv"),
            "w",
        ) as f:
            f.write(dimension_grammar_data)
        
        df2 = df[req_cols]
        df2 = df2.drop_duplicates(subset=indexed_col)
        if (len(dimension.fields) == 1):
            df2.insert(
                loc=0, column=f"{dimension.name}_id", value=range(1, len(df2) + 1)
            )

        df2.to_csv(
            os.path.join(dimensions_base_path, f"{dimension.name}-dimension.data.csv"),
            index=False,
        )


def write_events_to_ingest_folder(
    program_name: str, df: pd.DataFrame, metrics: typing.Iterable, dimensions: typing.Iterable, ingest_folder_path: str, dimensionFkCols: typing.Iterable, otherCols: typing.Iterable, column_metadata: typing.Dict
):
    events_base_path = os.path.join(ingest_folder_path, "programs", program_name)
    create_folder_if_not(events_base_path)

    for metric in metrics:
        dimension_name_row, dimension_col_row, type_row, column_row, last_row  = [], [], [], [], []
        for dimensionFkCol in dimensionFkCols:
            if column_metadata.get(dimensionFkCol['key']) is not None:
                columnName = column_metadata[dimensionFkCol['key']]['updated_col_name']
                typeOfDimension = getType(column_metadata[dimensionFkCol['key']]['type'])
            else:
                columnName = dimensionFkCol['key']
                typeOfDimension = 'number'

            dimension_name_row.append(dimensionFkCol['name'])
            dimension_col_row.append(columnName)
            type_row.append(typeOfDimension)
            column_row.append(columnName)
            last_row.append("dimension")

        for otherCol in otherCols:
            dimension_name_row.append("")
            dimension_col_row.append("")
            type_row.append(getType(column_metadata[otherCol]['type']))
            column_row.append(column_metadata[otherCol]['updated_col_name'])
            if (column_metadata[otherCol]['type'] == "datetime64"):
                last_row.append("timeDimension")
            else:
                last_row.append("")
        
        dimension_name_row.append("")
        dimension_col_row.append("")
        type_row.append(getType(column_metadata[metric]['type']))
        column_row.append(column_metadata[metric]['updated_col_name'])
        last_row.append("metric")

        event_grammar = f"""{','.join(dimension_name_row)}
{','.join(dimension_col_row)}
{','.join(type_row)}
{','.join(column_row)}
{','.join(last_row)}
"""
        
        with open(
            os.path.join(events_base_path, f"{metric}-event.grammar.csv"), "w"
        ) as f:
            f.write(event_grammar)


def write_config_to_ingest_folder(
    program_name, program_description, ingest_folder_path
):
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


def get_ingest_folder_path(token):
    program_folder_path = os.path.join(TMP_BASE_PATH, token, "ingest/programs")
    program_name = os.listdir(program_folder_path)[0]
    folder_path = os.path.join(program_folder_path, program_name)
    return folder_path


def get_events(token: str):
    return get_directory_structure(get_ingest_folder_path(token))


def download_ingest_folder(token: str):
    ingest_folder_path = os.path.join(TMP_BASE_PATH, token, "ingest")
    zip_location_path = os.path.join(TMP_BASE_PATH, token, "cqube-ingest")

    return shutil.make_archive(zip_location_path, "zip", ingest_folder_path)


def fetch_file_content(token: str, filename: str):
    if filename.endswith("event.data.csv") or filename.endswith("event.grammar.csv"):
        file_path = os.path.join(get_ingest_folder_path(token), filename)
    elif filename.endswith("dimension.data.csv") or filename.endswith(
        "dimension.grammar.csv"
    ):
        file_path = os.path.join(TMP_BASE_PATH, token, "ingest", "dimensions", filename)

    else:
        return None
    if not os.path.exists(file_path):
        return None
    df = pd.read_csv(file_path)
    json_response = df.to_json(orient="records")
    return json_response
