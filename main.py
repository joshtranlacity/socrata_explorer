# https://fastapi.tiangolo.com/deployment/deta/
from fastapi import FastAPI
from sodapy import Socrata
from enum import Enum

app = FastAPI()


class MetaDataType(Enum):
    FULL='FULL'
    COLUMNTYPES='COLUMNTYPES'
    COLUMNS='COLUMNS'
    INFO='INFO'


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/socrata/metadata")
def read_item(dataset_id: str = 'i5ke-k6by', metadatatype: MetaDataType=MetaDataType.FULL):
    client = Socrata(
        "data.lacity.org", None
    )
    results = client.get_metadata(dataset_id)
    columns = results['columns']
    if metadatatype==MetaDataType.FULL:
        return results
    elif metadatatype==MetaDataType.COLUMNS:
        for i, column in enumerate(columns):
            if 'cachedContents' in column:
                column['cachedContents'].pop('top', None)
            columns[i] = column
        return columns
    elif metadatatype==MetaDataType.INFO: 
        return {field:results[field] for field in results if type(results[field])!=list and type(results[field])!=dict}
    else:
        return {column['fieldName']:column['dataTypeName'] for column in columns if column['fieldName'].startswith(":")==False}


@app.get("/socrata/dataset")
def read_item(dataset_id: str = 'i5ke-k6by', limit: int=10, where: str="", order: str=""):
    client = Socrata(
        "data.lacity.org", None
    )
    results = results = client.get(dataset_id, limit=limit, where=where, order=order)
    return results

@app.get("/socrata/datasets")
def read_item(limit: int=10, order: str=""):
    client = Socrata(
        "data.lacity.org", None
    )
    results = results = client.datasets(limit=limit, order=order)
    return results