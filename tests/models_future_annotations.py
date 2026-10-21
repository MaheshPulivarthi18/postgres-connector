"""Models declared under PEP 563 string annotations (see test_string_annotations.py)."""
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from postgres_connector import Field, PostgreSQLModel, Relation


class FutAuthor(PostgreSQLModel):
    __table__ = "fut_authors"
    id:        int = Field(primary_key=True)
    name:      str = Field()
    born:      Optional[datetime] = Field()
    nickname:  Optional[str]
    rating:    Optional[float]
    joined:    datetime
    date:      date = Field(nullable=True)  # field named like its type
    books:     List[FutBook] = Relation(type="hasMany", foreign_key="author_id")


class FutBook(PostgreSQLModel):
    __table__ = "fut_books"
    id:        int = Field(primary_key=True)
    author_id: int = Field()
    author:    FutAuthor = Relation(type="belongsTo", foreign_key="author_id")
