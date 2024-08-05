import asyncio
from typing import Literal, Optional
from neo4j import AsyncDriver, AsyncGraphDatabase
from pydantic import BaseModel
import ssl
import models
from .. import datastore

NEO4J_IDENTIFIER = "neo4j"

class AuthConfig(BaseModel):
    username: str
    password: str

class Config(BaseModel, datastore.AbstractConfig):
    kind: Literal["neo4j"]
    uri: str
    auth: AuthConfig

class Client(datastore.Client[Config]):
    __driver: AsyncDriver

    @datastore.classproperty
    def kind(cls):
        return NEO4J_IDENTIFIER

    def __init__(self, driver):
        self.__driver = driver

    @property
    def driver(self):
        return self.__driver

    @classmethod
    async def create(cls, config: Config) -> "Client":
        return cls(
            AsyncGraphDatabase.driver(
                config.uri, auth=(config.auth.username, config.auth.password)
            )
        )

    async def initialize_data(
        self,
        airports: list[models.Airport],
        amenities: list[models.Amenity],
        flights: list[models.Flight],
        policies: list[models.Policy],
    ) -> None:
        async def delete_all(tx):
            await tx.run("MATCH (n) DETACH DELETE n")

        async def create_airport(tx, airport):
            await tx.run(
                """
                CREATE (a:Airport {id: $id, iata: $iata, name: $name, city: $city, country: $country})
                """,
                id=airport.id,
                iata=airport.iata,
                name=airport.name,
                city=airport.city,
                country=airport.country,
            )

        async def create_amenity(tx, amenity):
            await tx.run(
                """
                CREATE (a:Amenity {id: $id, name: $name, description: $description, location: $location, terminal: $terminal, category: $category, hour: $hour})
                """,
                id=amenity.id,
                name=amenity.name,
                description=amenity.description,
                location=amenity.location,
                terminal=amenity.terminal,
                category=amenity.category,
                hour=amenity.hour,
            )

        async def create_flight(tx, flight):
            await tx.run(
                """
                CREATE (f:Flight {id: $id, airline: $airline, flight_number: $flight_number, departure_airport: $departure_airport, arrival_airport: $arrival_airport, departure_time: $departure_time, arrival_time: $arrival_time, departure_gate: $departure_gate, arrival_gate: $arrival_gate})
                """,
                id=flight.id,
                airline=flight.airline,
                flight_number=flight.flight_number,
                departure_airport=flight.departure_airport,
                arrival_airport=flight.arrival_airport,
                departure_time=flight.departure_time,
                arrival_time=flight.arrival_time,
                departure_gate=flight.departure_gate,
                arrival_gate=flight.arrival_gate,
            )

        async def create_policy(tx, policy):
            await tx.run(
                """
                CREATE (p:Policy {id: $id, content: $content, embedding: $embedding})
                """,
                id=policy.id,
                content=policy.content,
                embedding=policy.embedding,
            )

        async with self.__driver.session() as session:
            # Delete all existing nodes and relationships
            await session.execute_write(delete_all)

            # Initialize data sequentially to avoid concurrency issues
            for airport in airports:
                await session.execute_write(create_airport, airport)

            for amenity in amenities:
                await session.execute_write(create_amenity, amenity)

            for flight in flights:
                await session.execute_write(create_flight, flight)

            for policy in policies:
                await session.execute_write(create_policy, policy)

    async def export_data(self) -> tuple[
        list[models.Airport],
        list[models.Amenity],
        list[models.Flight],
        list[models.Policy],
    ]:
        raise NotImplementedError("This client does not support export data.")

    async def get_airport_by_id(self, id: int) -> Optional[models.Airport]:
        raise NotImplementedError("This client does not support airports.")

    async def get_airport_by_iata(self, iata: str) -> Optional[models.Airport]:
        raise NotImplementedError("This client does not support airports.")

    async def search_airports(
        self,
        country: Optional[str] = None,
        city: Optional[str] = None,
        name: Optional[str] = None,
    ) -> list[models.Airport]:
        raise NotImplementedError("This client does not support airports.")

    async def get_amenity(self, id: int) -> Optional[models.Amenity]:
        async with self.__driver.session() as session:
            result = await session.run(
                "MATCH (amenity: Amenity {id: $id}) RETURN amenity", id=id
            )
            record = await result.single()

            if not record:
                return None

            amenity_data = record["amenity"]
            return models.Amenity(**amenity_data)

    async def amenities_search(
        self, query_embedding: list[float], similarity_threshold: float, top_k: int
    ) -> list[dict]:
        raise NotImplementedError("This client does not support amenities search.")

    async def get_flight(self, flight_id: int) -> Optional[models.Flight]:
        raise NotImplementedError("This client does not support flights.")

    async def search_flights_by_number(
        self, airline: str, flight_number: str
    ) -> list[models.Flight]:
        raise NotImplementedError("This client does not support flights.")

    async def search_flights_by_airports(
        self,
        date,
        departure_airport: Optional[str] = None,
        arrival_airport: Optional[str] = None,
    ) -> list[models.Flight]:
        raise NotImplementedError("This client does not support flights.")

    async def validate_ticket(
        self,
        airline: str,
        flight_number: str,
        departure_airport: str,
        departure_time: str,
    ) -> Optional[models.Flight]:
        raise NotImplementedError("This client does not support tickets.")

    async def insert_ticket(
        self,
        user_id: str,
        user_name: str,
        user_email: str,
        airline: str,
        flight_number: str,
        departure_airport: str,
        arrival_airport: str,
        departure_time: str,
        arrival_time: str,
    ):
        raise NotImplementedError("This client does not support tickets.")

    async def list_tickets(self, user_id: str) -> list[models.Ticket]:
        raise NotImplementedError("This client does not support tickets.")

    async def policies_search(
        self, query_embedding: list[float], similarity_threshold: float, top_k: int
    ) -> list[str]:
        raise NotImplementedError("This client does not support policies search.")

    async def close(self):
        try:
            await self.__driver.close()
        except ssl.SSLError as e:
            logging.error(f"SSL error occurred during closing: {e}")
