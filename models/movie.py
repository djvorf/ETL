import datetime
from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic.schema import date
from pydantic.types import UUID


class Movie(BaseModel):
    created: str
    modified: str
    id: str
    title: str
    description: str
    create_date: str
    age_qualification: int
    rating: float
    file: str
    category: str
    genres: Optional[List[str]]
    actors: Optional[List[str]]
    writers: Optional[List[str]]
    directors: Optional[List[str]]


