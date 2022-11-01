import re

# Regex patterns of status codes of Container service (https://github.com/nspcc-dev/neofs-spec/blob/98b154848116223e486ce8b43eaa35fec08b4a99/20-api-v2/container.md)
CONTAINER_NOT_FOUND = "code = 3072.*message = container not found"


# Regex patterns of status codes of Object service (https://github.com/nspcc-dev/neofs-spec/blob/98b154848116223e486ce8b43eaa35fec08b4a99/20-api-v2/object.md)
OBJECT_ACCESS_DENIED = "code = 2048.*message = access to object operation denied"
OBJECT_NOT_FOUND = "code = 2049.*message = object not found"
OBJECT_ALREADY_REMOVED = "code = 2052.*message = object already removed"
SESSION_NOT_FOUND = "code = 4096.*message = session token not found"
OUT_OF_RANGE = "code = 2053.*message = out of range"


def error_matches_status(error: Exception, status_pattern: str) -> bool:
    """
    Determines whether exception matches specified status pattern.

    We use re.search to be consistent with pytest.raises.
    """
    match = re.search(status_pattern, str(error))
    return match is not None
