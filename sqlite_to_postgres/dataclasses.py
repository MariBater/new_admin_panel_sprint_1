import logging
from dataclasses import dataclass, fields
from datetime import date, datetime
from uuid import UUID

logger = logging.getLogger(__name__)


@dataclass
class FilmWork:
    id: UUID
    title: str
    description: str | None
    creation_date: date | None
    rating: float | None
    type: str
    created: datetime
    modified: datetime

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == 'id' and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == date and isinstance(value, str):
                try:
                    setattr(self, field.name, datetime.strptime(value, '%Y-%m-%d').date())
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse date string '{value}' for {field.name} in FilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in FilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)
            elif field.name == 'rating' and value is not None:
                setattr(self, field.name, float(value))


@dataclass
class Person:
    id: UUID
    full_name: str
    created: datetime
    modified: datetime

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == 'id' and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in Person(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)


@dataclass
class Genre:
    id: UUID
    name: str
    description: str | None
    created: datetime
    modified: datetime

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == 'id' and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in Genre(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)


@dataclass
class GenreFilmWork:
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created: datetime

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name in ('id', 'film_work_id', 'genre_id') and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in GenreFilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)


@dataclass
class PersonFilmWork:
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created: datetime

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name in ('id', 'film_work_id', 'person_id') and isinstance(value, str):
                setattr(self, field.name, UUID(value))
            elif field.type == datetime and isinstance(value, str):
                try:
                    dt_value = value.replace('Z', '+00:00') if 'Z' in value else value
                    try:
                        setattr(self, field.name, datetime.fromisoformat(dt_value))
                    except ValueError:
                        setattr(self, field.name, datetime.strptime(dt_value, '%Y-%m-%d %H:%M:%S.%f%z' if '.' in dt_value and ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S%z' if ('+' in dt_value or '-' in dt_value[10:]) else '%Y-%m-%d %H:%M:%S.%f' if '.' in dt_value else '%Y-%m-%d %H:%M:%S'))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime string '{value}' for {field.name} in PersonFilmWork(id={self.id if hasattr(self, 'id') else 'N/A'}). Setting to None.")
                    setattr(self, field.name, None)

