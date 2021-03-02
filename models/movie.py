
from pydantic import BaseModel
from pydantic.schema import date, datetime
from pydantic.types import UUID


class Movie(BaseModel):
    created: datetime
    modified: datetime
    id: UUID
    title: str
    description: str
    created_date: date
    age_qualification: int
    rating: float
    file: str
    category_id: UUID

