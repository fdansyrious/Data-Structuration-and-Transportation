from typing import Optional
from sqlmodel import Field, SQLModel

class Teachers(SQLModel, table=True):
    TeacherID: str = Field(primary_key=True)
    TeacherName: str = Field(index=True)
    TeacherEmail: str = Field(index=True)
    TeacherPhone: Optional[str] = Field(default=None)