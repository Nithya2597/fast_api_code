from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List
import sqlite3
from geopy.distance import geodesic

# Initialize FastAPI app
app = FastAPI()


# Define models
class Address(BaseModel):
    id: int
    street: str
    city: str
    state: str
    country: str
    latitude: float
    longitude: float


# Initialize SQLite database connection
conn = sqlite3.connect('address_book.db', check_same_thread=False)
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''CREATE TABLE IF NOT EXISTS addresses 
                  (id INTEGER PRIMARY KEY, street TEXT, city TEXT, state TEXT, 
                   country TEXT, latitude REAL, longitude REAL)''')
conn.commit()


# API endpoints
@app.get("/addresses", response_model=List[Address])
def get_addresses():
    cursor.execute("SELECT * FROM addresses")
    addresses = cursor.fetchall()
    return [{"id": row[0], "street": row[1], "city": row[2], "state": row[3],
             "country": row[4], "latitude": row[5], "longitude": row[6]} for row in addresses]


@app.get("/addresses/addresses_within_distance", response_model=List[Address])
def get_addresses_within_distance(latitude: float = Query(..., description="Latitude of the location"),
                                  longitude: float = Query(..., description="Longitude of the location"),
                                  distance: float = Query(..., description="Distance in kilometers")):
    cursor.execute("SELECT * FROM addresses")
    addresses = cursor.fetchall()
    location = (latitude, longitude)
    addresses_within_distance = []
    for row in addresses:
        address_location = (row[5], row[6])  # (latitude, longitude)
        if geodesic(location, address_location).kilometers <= distance:
            addresses_within_distance.append({"id": row[0], "street": row[1],
                                              "city": row[2], "state": row[3],
                                              "country": row[4], "latitude": row[5],
                                              "longitude": row[6]})
    return addresses_within_distance


@app.post("/addresses")
def create_address(address: Address):
    cursor.execute("INSERT INTO addresses VALUES (?, ?, ?, ?, ?, ?, ?)",
                   (address.id, address.street, address.city, address.state,
                    address.country, address.latitude, address.longitude))
    conn.commit()
    return {"message": "Address created successfully"}


@app.put("/addresses/{address_id}")
def update_address(address_id: int, address: Address):
    cursor.execute("UPDATE addresses SET street=?, city=?, state=?, country=?, latitude=?, longitude=? WHERE id=?",
                   (address.street, address.city, address.state, address.country, address.latitude, address.longitude, address_id))
    conn.commit()
    return {"message": f"Address with id {address_id} updated successfully"}


@app.delete("/addresses/{address_id}")
def delete_address(address_id: int):
    cursor.execute("DELETE FROM addresses WHERE id=?", (address_id,))
    conn.commit()
    return {"message": f"Address with id {address_id} deleted successfully"}
