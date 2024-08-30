from typing import List, Optional
from typing_extensions import Annotated

from app.db import get_db as CouchbaseClient
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter()

HOTEL_COLLECTION = "hotel"
NAME_DESCRIPTION = "Hotel Name"
EXAMPLE_HOTEL_NAME = "Seal View"


class HotelName(BaseModel):
    """Model for Hotel Name"""

    name: Annotated[str, Field(description="Hotel Name")]


class Hotel(BaseModel):
    """Model for Hotels"""

    city: Optional[str] = Field(
        None, examples=["Santa Margarita"], description="Hotel Name"
    )
    country: Optional[str] = Field(
        None, examples=["United States"], description="Country Name"
    )
    description: Optional[str] = Field(
        None, examples=["newly renovated"], description="Description"
    )
    name: Optional[str] = Field(None, examples=["KCL Campground"], description="Name")
    state: Optional[str] = Field(None, examples=["California"], description="State")
    title: Optional[str] = Field(
        None, examples=["Carrizo Plain National Monument"], description="Title"
    )


@router.get(
    "/autocomplete",
    response_model=List[HotelName],
    description="Search for hotels based on their name. \n\n This provides an example of using [Search operations](https://docs.couchbase.com/python-sdk/current/howtos/full-text-searching-with-sdk.html#search-queries) in Couchbase to search for a specific name using the fts index.\n\n Code: [`api/hotel.py`](https://github.com/couchbase-examples/python-quickstart/blob/main/src/api/hotel.py) \n Method: `get`",
    responses={
        200: {
            "description": "List of Hotel Names",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def hotel_autocomplete(
    name: Annotated[
        str,
        Query(
            description=NAME_DESCRIPTION,
            examples=[EXAMPLE_HOTEL_NAME],
            openapi_examples={"Seal View": {"value": "Seal View"}},
        ),
    ],
    db=Depends(CouchbaseClient),
) -> List[HotelName]:
    """Hotel name with specified name"""
    try:
        result = db.search_by_name(name)
        return [{"name": name} for name in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@router.post(
    "/filter",
    response_model=List[Hotel],
    description="Filter hotels using various filters such as name, title, description, country, state and city. \n\n This provides an example of using [Search operations](https://docs.couchbase.com/python-sdk/current/howtos/full-text-searching-with-sdk.html#search-queries) in Couchbase to filter documents using the fts index.\n\n Code: [`api/hotel.py`](https://github.com/couchbase-examples/python-quickstart/blob/main/src/api/hotel.py) \n Method: `post`",
    responses={
        200: {
            "description": "List of Hotel",
        },
        500: {
            "description": "Unexpected Error",
        },
    },
)
def hotel_filter(
    hotel: Optional[Hotel] = None,
    limit: int = Query(10, description="Number of hotels to return (page size)"),
    offset: int = Query(0, description="Number of hotels to skip (for pagination)"),
    db=Depends(CouchbaseClient),
) -> List[Hotel]:
    """Hotel filter with various filters"""
    try:
        hotels = db.filter(
            hotel.model_dump(exclude_none=True), limit=limit, offset=offset
        )
        return hotels
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")
