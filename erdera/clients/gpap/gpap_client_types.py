"""Types for GPAP Production API Client"""
from typing import TypedDict
from enum import Enum

ApiHeaders = TypedDict(
    'ApiHeaders', {'Content-Type': str, 'Authorization': str})


class NameValue(TypedDict):
    """Generic name-value dict"""
    name: str

class IdValue(TypedDict):
    """Generic record set"""
    id: str
    value: list[str]


class Sorting(TypedDict):
    """Typing for sorted"""
    id: str
    desc: bool


class ApiBody(TypedDict):
    """Typing for API Body"""
    page: int
    pageSize: int
    fields: list[str]
    sorted: list[Sorting]
    filtered: list[IdValue]
    aggregates: list[str]

class ParticipantsResponse(TypedDict):
    """Typing for Participants response"""
    rows: list[dict]
    pages: int
    aggregations: dict
    total: int
    total_page: int


class ExperimentsMeta(TypedDict):
    """Meta tag in experiments response"""
    total_items: int
    total_pages: int
    page: int
    page_size: int


class ExperimentsResponse(TypedDict):
    """Typing for Experiments response"""
    items: list[dict]
    _meta: ExperimentsMeta


class MetadataTypes(Enum):
    """GPAP API Metadata types"""
    participants: str
    experiments: str

class ApiRequestBodyFields(TypedDict):
    """Typing for body.fields"""
    participants: list[str]
    experiments: list[str]

JobErrors = TypedDict(
    'JobErrors',
    {
        'id': str,
        'job': str,
        'type': str,
        'message': str
    }
)

class JobOutput(TypedDict):
    """Job output data structure"""
    data: list[dict]
    errors: list[JobErrors]
    errorCount: int

JobsGpapApi = TypedDict(
    'JobsGpapApi',
    {
        'id': str,
        'date of run': str,
        'ok': bool,
        'total number of participants': int,
        'number of new participants': int,
        'number of updated participants': int,
        'total number of experiments': int,
        'number of new experiments': int,
        'number of updated experiments': int,
        'number of errors': int
    }
)
