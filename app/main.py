from fastapi import FastAPI
from contextlib import asynccontextmanager
from starlette.responses import RedirectResponse
from app.routers import airline, airport, route, hotel
from app.db import get_db


# Initialize couchbase connection
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Method that gets called upon app initialization to initialize couchbase connection & close the connection on exit"""
    db = get_db()
    yield
    db.close()


app = FastAPI(
    title="Python Quickstart using FastAPI",
    version="1.0.0",
    description="""
    A quickstart API using Python with Couchbase, FastAPI & travel-sample data.

    We have a visual representation of the API documentation using Swagger which allows you to interact with the API's endpoints directly through the browser. It provides a clear view of the API including endpoints, HTTP methods, request parameters, and response objects. 
    
    Click on an individual endpoint to expand it and see detailed information. This includes the endpoint's description, possible response status codes, and the request parameters it accepts.

    Trying Out the API
    You can try out an API by clicking on the "Try it out" button next to the endpoints.
    
    - Parameters: If an endpoint requires parameters, Swagger UI provides input boxes for you to fill in. This could include path parameters, query strings, headers, or the body of a POST/PUT request.

    - Execution: Once you've inputted all the necessary parameters, you can click the "Execute" button to make a live API call. Swagger UI will send the request to the API and display the response directly in the documentation. This includes the response code, response headers, and response body.

    Models
    Swagger documents the structure of request and response bodies using models. These models define the expected data structure using JSON schema and are extremely helpful in understanding what data to send and expect.

    For details on the API, please check the tutorial on the Couchbase Developer Portal: https://developer.couchbase.com/tutorial-quickstart-fastapi-python/
    """,
    lifespan=lifespan,
)


app.include_router(airline.router, tags=["airline"], prefix="/api/v1/airline")
app.include_router(airport.router, tags=["airport"], prefix="/api/v1/airport")
app.include_router(route.router, tags=["route"], prefix="/api/v1/route")
app.include_router(hotel.router, tags=["hotel"], prefix="/api/v1/hotel")


# Redirect to Swagger documentation on loading the API for demo purposes
@app.get("/", include_in_schema=False)
def redirect_to_swagger():
    return RedirectResponse(url="/docs")
