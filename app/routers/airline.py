from typing import Union
from typing_extensions import Annotated

from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from app.db import get_db
from fastapi import APIRouter, Depends, HTTPException, Path, status, Query
from pydantic import BaseModel, Field

router = APIRouter()
AIRLINE_COLLECTION = "airline"
ID_DESCRIPTION = "Airline ID"
EXAMPLE_AIRLINE_ID = "airline_10"
EXAMPLE_COUNTRY = "United States"
EXAMPLE_AIRPORT = "SFO"


class Airline(BaseModel):
    """Model for Airline"""

    name: Annotated[str, Field(examples=["Sample Airline"], description="Airline Name")]
    iata: Annotated[
        Union[str, None], Field(examples=["SA"], description="IATA code")
    ] = None
    icao: Annotated[
        Union[str, None], Field(examples=["SAA"], description="ICAO code")
    ] = None
    callsign: Annotated[
        Union[str, None], Field(examples=["SAF"], description="Callsign")
    ] = None
    country: Annotated[str, Field(examples=["United States"], description="Country")]


@router.get(
    "/list",
    response_model=list[Airline],
    description="Get a list of airlines with pagination. Optionally, you can filter the list by Country. \n\n This provides an example of using [SQL++ query](https://docs.couchbase.com/python-sdk/current/howtos/n1ql-queries-with-sdk.html) in Couchbase to fetch a list of documents matching the specified criteria.\n\n Code: [`routers/airline.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/airline.py) \n\n Method: `get_airlines_list`",
    responses={
        500: {
            "description": "Unexpected Error",
        },
    },
)
def get_airlines_list(
    country: Annotated[
        Union[str, None],
        Query(
            description="Country",
            examples=[EXAMPLE_COUNTRY],
            openapi_examples={
                "All": {"value": ""},
                "France": {"value": "France"},
                "United States": {"value": "United States"},
            },
        ),
    ] = None,
    limit: Annotated[
        Union[int, None], Query(description="Number of airlines to return (page size)")
    ] = 10,
    offset: Annotated[
        Union[int, None],
        Query(description="Number of airlines to skip (for pagination)"),
    ] = 0,
    db=Depends(get_db),
) -> list[Airline]:
    """Get a list of airlines with pagination. Optionally, filter by country."""
    if country:
        query = """
            SELECT airline.callsign,
                airline.country,
                airline.iata,
                airline.icao,
                airline.name
            FROM airline as airline 
            WHERE airline.country=$country 
            ORDER BY airline.name
            LIMIT $limit 
            OFFSET $offset;
        """

    else:
        query = """
            SELECT airline.callsign,
                airline.country,
                airline.iata,
                airline.icao,
                airline.name
            FROM airline as airline 
            ORDER BY airline.name
            LIMIT $limit 
            OFFSET $offset;
        """

    try:
        result = db.query(query, country=country, limit=limit, offset=offset)
        airlines = [r for r in result]
        return airlines
    except Exception as e:
        return f"Unexpected error: {e}", 500


@router.get(
    "/to-airport",
    response_model=list[Airline],
    description="Get Airlines flying to specified destination Airport. \n\n This provides an example of using [SQL++ query](https://docs.couchbase.com/python-sdk/current/howtos/n1ql-queries-with-sdk.html) in Couchbase to fetch a list of documents matching the specified criteria.\n\n Code: [`routers/airline.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/airline.py) \n\n Method: `get_airlines_to_airport`",
    responses={
        500: {
            "description": "Unexpected Error",
        }
    },
)
def get_airlines_to_airport(
    airport: Annotated[
        str,
        Query(
            description="Destination Airport",
            examples=[EXAMPLE_AIRPORT],
            openapi_examples={
                "SFO": {"value": "SFO"},
                "JFK": {"value": "JFK"},
                "LHR": {"value": "LHR"},
            },
        ),
    ],
    limit: Annotated[
        Union[int, None], Query(description="Number of airlines to return (page size)")
    ] = 10,
    offset: Annotated[
        Union[int, None],
        Query(description="Number of airlines to skip (for pagination)"),
    ] = 0,
    db=Depends(get_db),
) -> list[Airline]:
    """Get a list of airlines that fly to the specified airport."""
    try:
        query = """
            SELECT air.callsign,
                air.country,
                air.iata,
                air.icao,
                air.name
            FROM (
                SELECT DISTINCT META(airline).id AS airlineId
                FROM route
                JOIN airline ON route.airlineid = META(airline).id
                WHERE route.destinationairport = $airport
            ) AS subquery
            JOIN airline AS air ON META(air).id = subquery.airlineId
            ORDER BY air.name
            LIMIT $limit 
            OFFSET $offset;
        """
        result = db.query(query, airport=airport, limit=limit, offset=offset)
        airlines = [r for r in result]
        return airlines
    except Exception as e:
        return HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.get(
    "/{id}",
    response_model=Airline,
    description="Get Airline with specified ID. \n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to get a document with specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/airline.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/airline.py) \n\n Method: `read_airline`",
    responses={
        404: {
            "description": "Airline not found",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def read_airline(
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRLINE_ID],
            openapi_examples={
                "airline_10": {"value": "airline_10"},
            },
        ),
    ],
    db=Depends(get_db),
) -> Airline:
    """Get Airline with specified ID"""
    try:
        return db.get_document(AIRLINE_COLLECTION, id).content_as[dict]
    except DocumentNotFoundException:
        raise HTTPException(status_code=404, detail="Airline not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.post(
    "/{id}",
    response_model=Airline,
    description="Create Airline with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to create a new document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/airline.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/airline.py) \n\n Method: `create_airline`",
    status_code=status.HTTP_201_CREATED,
    responses={
        409: {
            "description": "Airline already exists",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def create_airline(
    airline: Airline,
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRLINE_ID],
            openapi_examples={
                "airline_10": {"value": "airline_10"},
            },
        ),
    ],
    db=Depends(get_db),
) -> Airline:
    """Create Airline with specified ID"""
    try:
        db.insert_document(AIRLINE_COLLECTION, id, airline.model_dump())
        return airline
    except DocumentExistsException:
        raise HTTPException(status_code=409, detail="Airline already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.put(
    "/{id}",
    response_model=Airline,
    description="Update Airline with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to upsert a document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/airline.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/airline.py) \n\n Method: `update_airline`",
    responses={
        200: {
            "description": "Airline Updated",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def update_airline(
    airline: Airline,
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRLINE_ID],
            openapi_examples={
                "airline_10": {"value": "airline_10"},
            },
        ),
    ],
    db=Depends(get_db),
) -> Airline:
    """Update Airline with specified ID"""
    try:
        db.upsert_document(AIRLINE_COLLECTION, id, airline.model_dump())
        return airline
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.delete(
    "/{id}",
    status_code=status.HTTP_204_NO_CONTENT,
    description="Delete Airline with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to delete a document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/airline.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/airline.py) \n\n Method: `delete_airline`",
    responses={
        404: {
            "description": "Airline not found",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def delete_airline(
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRLINE_ID],
            openapi_examples={
                "airline_10": {"value": "airline_10"},
            },
        ),
    ],
    db=Depends(get_db),
) -> None:
    """Delete Airline with specified ID"""
    try:
        db.delete_document(AIRLINE_COLLECTION, id)
    except DocumentNotFoundException:
        raise HTTPException(status_code=404, detail="Airline not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
