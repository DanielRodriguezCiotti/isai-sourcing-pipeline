import threading

from supabase import Client, create_client

from .settings import get_settings

_SUPABASE_CLIENT = None
_supabase_client_lock = threading.Lock()


def get_supabase_client() -> Client:
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        with _supabase_client_lock:
            settings = get_settings()
            _SUPABASE_CLIENT = create_client(
                settings.supabase_url,
                settings.supabase_service_role_key.get_secret_value(),
            )
    return _SUPABASE_CLIENT
