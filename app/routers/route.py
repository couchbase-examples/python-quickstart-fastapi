from typing import Union
from typing_extensions import Annotated

from couchbase.exceptions import DocumentExistsException, DocumentNotFoundException
from app.db import get_db as CouchbaseClient
from fastapi import APIRouter, Depends, HTTPException, Path, status
from pydantic import BaseModel, Field

router = APIRouter()

ROUTE_COLLECTION = "route"
ID_DESCRIPTION = "route ID"
EXAMPLE_ROUTE_ID = "route_10000"


class Schedule(BaseModel):
    """Model for Schedules"""

    day: Annotated[int, Field(examples=[0], description="Day of week")]
    flight: Annotated[str, Field(examples=["AF10"], description="Flight Number")]
    utc: Annotated[str, Field(examples=["10:05:00"], description="UTC time")]


class Route(BaseModel):
    """Model for Route"""

    airline: Annotated[str, Field(examples=["AF"], description="Airline")]
    airlineid: Annotated[str, Field(examples=["airline_10"], description="Airline ID")]
    sourceairport: Annotated[
        str, Field(examples=["SFO"], description="Source Airport IATA code")
    ]
    destinationairport: Annotated[
        str, Field(examples=["JFK"], description="Destination Airport IATA code")
    ]
    stops: Annotated[
        Union[int, None], Field(examples=[0], description="Number of stops")
    ] = None
    equipment: Annotated[
        Union[str, None], Field(description="Equipment", examples=["320"])
    ] = None
    schedule: Annotated[
        Union[list[Schedule], None], Field(description="Schedules")
    ] = None
    distance: Annotated[
        Union[float, None], Field(examples=[4151.79], description="Distance in km")
    ] = None


@router.get(
    "/{id}",
    response_model=Route,
    description="Get route with specified ID. \n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to get a document with specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/route.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/route.py) \n\n Method: `read_route`",
    responses={
        404: {
            "description": "Route not found",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def read_route(
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_ROUTE_ID],
            openapi_examples={"route_10000": {"value": "route_10000"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> Route:
    """Get route with specified ID"""
    try:
        return db.get_document(ROUTE_COLLECTION, id).content_as[dict]
    except DocumentNotFoundException:
        raise HTTPException(status_code=404, detail="Route not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.post(
    "/{id}",
    response_model=Route,
    description="Create route with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to create a new document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/route.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/route.py) \n\n Method: `create_route`",
    status_code=status.HTTP_201_CREATED,
    responses={
        409: {
            "description": "route already exists",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def create_route(
    route: Route,
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_ROUTE_ID],
            openapi_examples={"route_10000": {"value": "route_10000"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> Route:
    """Create route with specified ID"""
    try:
        db.insert_document(ROUTE_COLLECTION, id, route.model_dump())
        return route
    except DocumentExistsException:
        raise HTTPException(status_code=409, detail="route already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.put(
    "/{id}",
    response_model=Route,
    description="Update route with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to upsert a document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/route.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/route.py) \n\n Method: `update_route`",
    responses={
        200: {
            "description": "Route Updated",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def update_route(
    route: Route,
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_ROUTE_ID],
            openapi_examples={"route_10000": {"value": "route_10000"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> Route:
    """Update route with specified ID"""
    try:
        db.upsert_document(ROUTE_COLLECTION, id, route.model_dump())
        return route
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.delete(
    "/{id}",
    status_code=status.HTTP_204_NO_CONTENT,
    description="Delete route with specified ID.\n\n This provides an example of using [Key Value operations](https://docs.couchbase.com/python-sdk/current/howtos/kv-operations.html) in Couchbase to delete a document with a specified ID.\n\n Key Value operations are unique to Couchbase and provide very high speed get/set/delete operations.\n\n Code: [`routers/route.py`](https://github.com/couchbase-examples/python-quickstart-fastapi/blob/main/app/routers/route.py) \n\n Method: `delete_route`",
    responses={
        404: {
            "description": "Route not found",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def delete_route(
    id: Annotated[
        str,
        Path(
            description=ID_DESCRIPTION,
            examples=[EXAMPLE_ROUTE_ID],
            openapi_examples={"route_10000": {"value": "route_10000"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> None:
    """Delete route with specified ID"""
    try:
        db.delete_document(ROUTE_COLLECTION, id)
    except DocumentNotFoundException:
        raise HTTPException(status_code=404, detail="Route not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
