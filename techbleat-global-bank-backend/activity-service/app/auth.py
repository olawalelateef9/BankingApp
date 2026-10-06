"""OIDC bearer-token validation (Cognito or Keycloak).

oauth2-proxy logs the browser in and ingress-nginx forwards
`Authorization: Bearer <id_token>` to this service. We verify that token
here rather than trusting the ingress alone.

Auth is enabled when OIDC_ISSUER is set (Kubernetes injects it from the
`oidc-config` ConfigMap). Without it, as in local docker-compose, requests
are not authenticated.
"""
import json
import logging
import os
import urllib.request
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import jwt
from fastapi import Header, HTTPException
from sqlalchemy import text

logger = logging.getLogger(__name__)

OIDC_ISSUER = os.getenv("OIDC_ISSUER", "").rstrip("/")
OIDC_AUDIENCE = os.getenv("OIDC_AUDIENCE") or None
OIDC_ADMIN_GROUP = os.getenv("OIDC_ADMIN_GROUP", "bank-admins")
OIDC_GROUPS_CLAIM = os.getenv("OIDC_GROUPS_CLAIM", "cognito:groups")

if not OIDC_ISSUER:
    logger.warning("OIDC_ISSUER is not set - API authentication is DISABLED")


@dataclass
class Caller:
    email: str
    groups: list

    @property
    def is_admin(self) -> bool:
        return OIDC_ADMIN_GROUP in self.groups


@lru_cache(maxsize=1)
def _jwks_client() -> jwt.PyJWKClient:
    discovery_url = f"{OIDC_ISSUER}/.well-known/openid-configuration"
    with urllib.request.urlopen(discovery_url, timeout=5) as response:
        jwks_uri = json.load(response)["jwks_uri"]
    return jwt.PyJWKClient(jwks_uri, cache_keys=True)


def get_caller(authorization: Optional[str] = Header(None)) -> Optional[Caller]:
    """FastAPI dependency: the authenticated caller, or None when auth is disabled."""
    if not OIDC_ISSUER:
        return None

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1]
    try:
        signing_key = _jwks_client().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            issuer=OIDC_ISSUER,
            audience=OIDC_AUDIENCE,
            options={"verify_aud": OIDC_AUDIENCE is not None, "require": ["exp", "iss"]},
        )
    except jwt.PyJWTError as exc:
        logger.warning(f"Rejected bearer token: {exc}")
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    email = claims.get("email")
    if not email:
        raise HTTPException(status_code=401, detail="Token has no email claim")

    return Caller(email=email, groups=list(claims.get(OIDC_GROUPS_CLAIM) or []))


def require_access(caller: Optional[Caller], user_id: str, engine) -> None:
    """Allow admins, or the caller whose registered email owns user_id."""
    if caller is None or caller.is_admin:
        return

    with engine.begin() as conn:
        own_id = conn.execute(
            text("SELECT id FROM users WHERE lower(email) = lower(:email)"),
            {"email": caller.email},
        ).scalar()

    if own_id != user_id:
        raise HTTPException(status_code=403, detail="Not allowed to access this account")
