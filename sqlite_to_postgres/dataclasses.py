import logging
from dataclasses import dataclass, fields
from datetime import date, datetime
from uuid import UUID
import functools

logger = logging.getLogger(__name__)


@functools.lru_cache
def _parse_datetime(value: str) -> datetime | None:
    """A cached helper to parse various datetime string formats."""
    if not isinstance(value, str):
        return None

    # Normalize 'Z' to a valid UTC offset
    dt_value = value.replace('Z', '+00:00')

    # Try the standard and fast ISO 8601 format first
    try:
        return datetime.fromisoformat(dt_value)
    except ValueError:
        pass

    # If that fails, try a list of common formats
    formats_to_try = [
        '%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S%z',
        '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
    ]
    for fmt in formats_to_try:
        try:
            return datetime.strptime(dt_value, fmt)
        except ValueError:
            continue
    return None


@dataclass
class BaseDataClass:
    """A base class for dataclasses that handles common type conversions."""
    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if value is None:
                continue

            # Get the actual type for Union types like `str | None`
            field_type = field.type
            if hasattr(field_type, '__args__'):
                # Handles Optional[T] which is Union[T, None]
                actual_types = [t for t in field_type.__args__ if t is not type(None)]
                if len(actual_types) == 1:
                    field_type = actual_types[0]

            try:
                if field_type == UUID and isinstance(value, str):
                    setattr(self, field.name, UUID(value))
                elif field_type == datetime and isinstance(value, str):
                    parsed_dt = _parse_datetime(value)
                    if parsed_dt:
                        setattr(self, field.name, parsed_dt)
                    else:
                        raise ValueError(f"Could not parse datetime string '{value}'")
                elif field_type == date and isinstance(value, str):
                    setattr(self, field.name, datetime.strptime(value, '%Y-%m-%d').date())
                elif field_type == float and not isinstance(value, float):
                    setattr(self, field.name, float(value))
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Could not convert value '{value}' for field '{field.name}' in {self.__class__.__name__}"
                    f"(id={getattr(self, 'id', 'N/A')}). Error: {e}. Setting to None."
                )
                setattr(self, field.name, None)


@dataclass
class FilmWork(BaseDataClass):
    id: UUID
    title: str
    description: str | None
    creation_date: date | None
    rating: float | None
    type: str
    created: datetime
    modified: datetime


@dataclass
class Person(BaseDataClass):
    id: UUID
    full_name: str
    created: datetime
    modified: datetime


@dataclass
class Genre(BaseDataClass):
    id: UUID
    name: str
    description: str | None
    created: datetime
    modified: datetime


@dataclass
class GenreFilmWork(BaseDataClass):
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created: datetime


@dataclass
class PersonFilmWork(BaseDataClass):
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created: datetime
