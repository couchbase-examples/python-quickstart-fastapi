from typing import Union
from typing_extensions import Annotated

from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from app.db import get_db as CouchbaseClient
from fastapi import APIRouter, Depends, HTTPException, Path, status, Query
from pydantic import BaseModel, Field

router = APIRouter()

AIRPORT_COLLECTION = "airport"
ID_DESCRIPTION = "airport ID"
EXAMPLE_AIRPORT_ID = "airport_1273"
EXAMPLE_COUNTRY = "France"
DIRECT_CONNECTIONS_DESCRIPTION = "Source Airport FAA Code"
EXAMPLE_AIRPORTS = "JFK"


class GeoCoordinates(BaseModel):
    """Model for Geo Coordinates"""

    lat: Annotated[float, Field(examples=[48.864716], description="Latitude")]
    lon: Annotated[float, Field(examples=[2.349014], description="Longitude")]
    alt: Annotated[float, Field(examples=[92.0], description="Altitude")]


class Airport(BaseModel):
    """Model for Airport"""

    airportname: Annotated[
        str, Field(examples=["Sample airport"], description="Airport Name")
    ]
    city: Annotated[str, Field(examples=["Sample City"], description="City")]
    country: Annotated[str, Field(examples=["United Kingdom"], description="Country")]
    faa: Annotated[
        Union[str, None], Field(examples=["SAA"], description="FAA code")
    ] = None
    icao: Annotated[
        Union[str, None], Field(examples=["SAA"], description="ICAO code")
    ] = None
    tz: Annotated[
        Union[str, None], Field(examples=["Europe/Paris"], description="Timezone")
    ] = None
    geo: Annotated[
        Union[GeoCoordinates, None], Field(description="Geo coordinates")
    ] = None


@router.get(
    "/list",
    response_model=list[Airport],
    description="Get a list of airports with pagination. Optionally, you can filter the list by Country. \n\n This provides an example of using [SQL++ query](https://docs.couchbase.com/python-sdk/current/howtos/n1ql-queries-with-sdk.html) in Couchbase to fetch a list of documents matching the specified criteria.\n\n Code: `routers/airport.py`\n\n Method: `get_airports_list`",
    responses={
        500: {
            "description": "Unexpected Error",
        },
    },
)
def get_airports_list(
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
        Union[int, None], Query(description="Number of airports to return (page size)")
    ] = 10,
    offset: Annotated[
        Union[int, None],
        Query(description="Number of airports to skip (for pagination)"),
    ] = 0,
    db=Depends(CouchbaseClient),
) -> list[Airport]:
    """Get a list of airports with pagination. Optionally, filter by country."""
    if country:
        query = """
            SELECT airport.airportname,
                airport.city,
                airport.country,
                airport.faa,
                airport.geo,
                airport.icao,
                airport.tz
            FROM airport AS airport
            WHERE airport.country = $country
            ORDER BY airport.airportname
            LIMIT $limit
            OFFSET $offset;
            """
    else:
        query = """
            SELECT airport.airportname,
                airport.city,
                airport.country,
                airport.faa,
                airport.geo,
                airport.icao,
                airport.tz
            FROM airport AS airport
            ORDER BY airport.airportname
            LIMIT $limit
            OFFSET $offset;
            """

    try:
        result = db.query(query, country=country, limit=limit, offset=offset)
        airports = [r for r in result]
        return airports
    except Exception as e:
        return f"Unexpected error: {e}", 500


class DestinationAirport(BaseModel):
    """Model for Destination Airport"""

    destinationairport: Annotated[
        str, Field(examples=["JFK"], description="Airport FAA code")
    ]


@router.get(
    "/direct-connections",
    response_model=list[DestinationAirport],
    summary="Get Direct Connections",
    description="Get Direct Connections from specified Airport. \n\n This provides an example of using [SQL++ query](https://docs.couchbase.com/python-sdk/current/howtos/n1ql-queries-with-sdk.html) in Couchbase to fetch a list of documents matching the specified criteria.\n\n Code: `routers/airport.py`\n\n Method: `get_airports_direct_connections`",
    responses={
        500: {
            "description": "Unexpected Error",
        }
    },
)
def get_airport_direct_connections(
    airport: Annotated[
        str,
        Query(
            description=DIRECT_CONNECTIONS_DESCRIPTION,
            examples=[EXAMPLE_AIRPORTS],
            openapi_examples={
                "JFK": {"value": "JFK"},
                "SFO": {"value": "SFO"},
                "LHR": {"value": "LHR"},
            },
        ),
    ],
    limit: Annotated[
        Union[int, None],
        Query(description="Number of direct connections to return (page size)"),
    ] = 10,
    offset: Annotated[
        Union[int, None],
        Query(description="Number of direct connections to skip (for pagination)"),
    ] = 0,
    db=Depends(CouchbaseClient),
) -> list[DestinationAirport]:
    """Get a list of airports that fly to the specified airport."""
    try:
        query = """
            SELECT distinct (route.destinationairport)
                FROM airport as airport
                JOIN route as route on route.sourceairport = airport.faa
                WHERE airport.faa = $airport and route.stops = 0
                ORDER BY route.destinationairport
                LIMIT $limit
                OFFSET $offset
        """
        result = db.query(query, airport=airport, limit=limit, offset=offset)
        airports = [r for r in result]
        return airports
    except Exception as e:
        return HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.get(
    "/{id}",
    response_model=Airport,
    description="Get airport with specified ID. \n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to get a document with specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: `routers/airport.py` \n\n Method: `read_airport`",
    responses={
        404: {
            "description": "Airport not found",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def read_airport(
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRPORT_ID],
            openapi_examples={"airport_1273": {"value": "airport_1273"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> Airport:
    """Get airport with specified ID"""
    try:
        return db.get_document(AIRPORT_COLLECTION, id).content_as[dict]
    except DocumentNotFoundException:
        raise HTTPException(status_code=404, detail="Airport not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.post(
    "/{id}",
    response_model=Airport,
    description="Create airport with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to create a new document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: `routers/airport.py` \n\n Method: `create_airport`",
    status_code=status.HTTP_201_CREATED,
    responses={
        409: {
            "description": "Airport already exists",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def create_airport(
    airport: Airport,
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRPORT_ID],
            openapi_examples={"airport_1273": {"value": "airport_1273"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> Airport:
    """Create airport with specified ID"""
    try:
        db.insert_document(AIRPORT_COLLECTION, id, airport.model_dump())
        return airport
    except DocumentExistsException:
        raise HTTPException(status_code=409, detail="airport already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.put(
    "/{id}",
    response_model=Airport,
    description="Update airport with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to upsert a document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: `routers/airport.py` \n\n Method: `update_airport`",
    responses={
        200: {
            "description": "Airport Updated",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def update_airport(
    airport: Airport,
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRPORT_ID],
            openapi_examples={"airport_1273": {"value": "airport_1273"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> Airport:
    """Update airport with specified ID"""
    try:
        db.upsert_document(AIRPORT_COLLECTION, id, airport.model_dump())
        return airport
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.delete(
    "/{id}",
    status_code=status.HTTP_204_NO_CONTENT,
    description="Delete airport with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to delete a document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: `routers/airport.py` \n\n Method: `delete_airport`",
    responses={
        404: {
            "description": "Airport not found",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def delete_airport(
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_AIRPORT_ID],
            openapi_examples={"airport_1273": {"value": "airport_1273"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> None:
    """Delete airport with specified ID"""
    try:
        db.delete_document(AIRPORT_COLLECTION, id)
    except DocumentNotFoundException:
        raise HTTPException(status_code=404, detail="Airport not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
