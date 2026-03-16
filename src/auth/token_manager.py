import requests
import time
from typing import Optional, Dict
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class TokenInfo:
    access_token: str
    expires_in: int
    token_type: str
    obtained_at: datetime

class TokenManager:
    def __init__(self, client_id: str, client_secret: str, token_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self._current_token: Optional[TokenInfo] = None
        self._lock = False
        
    def get_token(self) -> str:
        if self._token_valid():
            return self._current_token.access_token
        return self._refresh_token()
    
    def _token_valid(self) -> bool:
        if not self._current_token:
            return False
        expiry_time = self._current_token.obtained_at + timedelta(
            seconds=self._current_token.expires_in - 300
        )
        return datetime.now() < expiry_time
    
    def _refresh_token(self) -> str:
        if self._lock:
            time.sleep(1)
            return self._current_token.access_token if self._current_token else self._refresh_token()
        
        try:
            self._lock = True
            logger.info("Obtendo novo token")
            
            response = requests.post(
                self.token_url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30
            )
            response.raise_for_status()
            
            token_data = response.json()
            
            self._current_token = TokenInfo(
                access_token=token_data["access_token"],
                expires_in=token_data["expires_in"],
                token_type=token_data.get("token_type", "Bearer"),
                obtained_at=datetime.now()
            )
            
            return self._current_token.access_token
            
        finally:
            self._lock = False