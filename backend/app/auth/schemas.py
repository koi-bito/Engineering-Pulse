import uuid
from datetime import datetime

from fastapi_users import schemas
from pydantic import BaseModel, EmailStr, Field


class UserRead(schemas.BaseUser[uuid.UUID]):
    org_id: uuid.UUID | None = None
    team_id: uuid.UUID | None = None
    role: str = "member"
    created_at: datetime | None = None


class UserCreate(schemas.BaseUserCreate):
    org_id: uuid.UUID | None = None
    team_id: uuid.UUID | None = None
    role: str = "member"


class UserUpdate(schemas.BaseUserUpdate):
    org_id: uuid.UUID | None = None
    team_id: uuid.UUID | None = None
    role: str | None = None


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    org_name: str = Field(min_length=2, max_length=100)
    team_name: str = Field(min_length=2, max_length=100)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserRead | None = None

