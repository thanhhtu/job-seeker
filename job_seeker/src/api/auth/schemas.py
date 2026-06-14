from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class UserRegister(BaseModel):
    email: str = Field(..., min_length=3, max_length=254)
    password: str = Field(..., min_length=8, max_length=128)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        e = v.strip().lower()
        if "@" not in e:
            raise ValueError("Invalid email.")
        return e


class UserLogin(BaseModel):
    email: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        return v.strip().lower()


class UserPublic(BaseModel):
    id: str
    email: str
    name: str | None = None
    phone: str | None = None


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int
    user: UserPublic


class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=1)


class UpdateProfileRequest(BaseModel):
    name: str | None = Field(None, max_length=120)
    phone: str | None = Field(None, max_length=32)

    @field_validator("name", "phone")
    @classmethod
    def blank_to_none(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=128)
