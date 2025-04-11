"""
Environment utility functions.

This module contains environment utility functions.
"""
import logging
import os
from enum import Enum
from typing import TypeVar, Type

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=Enum)


######################################################################
#  ENVIRONMENT UTILITY FUNCTIONS
######################################################################
def get_enum_from_env(
        enum_type: Type[T],
        env_var_name: str,
        default_value: T
) -> T:
    """Retrieves an enum value from an environment variable.

    This helper function attempts to retrieve a value from the specified
    environment variable and convert it to the corresponding enum member.
    If the environment variable is not set or the value is invalid,
    it returns the provided default value.

    The function supports two ways of specifying enum values:
    1. Using the enum value (e.g., 'ROLE_ADMIN' for UserRole.ADMIN)
    2. Using the enum member name (case-insensitive, e.g., 'admin'
    for UserRole.ADMIN)

    Args:
        enum_type (Type[T]): The enum type to convert the environment variable
        value to.
        env_var_name (str): The name of the environment variable to retrieve.
        default_value (T): The default enum value to return if the environment
            variable is not set or the value is invalid.

    Returns:
        T: The enum member corresponding to the environment variable value, or
            the default value if the environment variable is not set or the
            value is invalid.
            
    Usage:
        # Basic usage with default value
        from enum import Enum
        
        class UserRole(Enum):
            ADMIN = 'ROLE_ADMIN'
            USER = 'ROLE_USER'
            GUEST = 'ROLE_GUEST'
            
        # Using enum value
        role = get_enum_from_env(UserRole, 'USER_ROLE', UserRole.USER)
        # If USER_ROLE=ROLE_ADMIN, role will be UserRole.ADMIN
        # If USER_ROLE=admin, role will be UserRole.ADMIN (case-insensitive)
        # If USER_ROLE is not set, role will be UserRole.USER
        
        # Using in a configuration class
        class AppConfig:
            def __init__(self):
                self.user_role = get_enum_from_env(UserRole, 'USER_ROLE', UserRole.USER)
                self.log_level = get_enum_from_env(LogLevel, 'LOG_LEVEL', LogLevel.INFO)
                self.environment = get_enum_from_env(Environment, 'ENV', Environment.DEVELOPMENT)
                
        # Using in a FastAPI application
        from fastapi import FastAPI
        from cba_core_lib.utils.env_utils import get_enum_from_env
        
        app = FastAPI()
        
        # Configure application based on environment variables
        app.environment = get_enum_from_env(Environment, 'APP_ENV', Environment.PRODUCTION)
        app.log_level = get_enum_from_env(LogLevel, 'APP_LOG_LEVEL', LogLevel.INFO)
        
        # Using in a Docker environment
        # In your Dockerfile or docker-compose.yml:
        # environment:
        #   - USER_ROLE=ROLE_ADMIN
        #   - LOG_LEVEL=DEBUG
        #   - ENV=PRODUCTION
        
        # Then in your application:
        user_role = get_enum_from_env(UserRole, 'USER_ROLE', UserRole.USER)
        log_level = get_enum_from_env(LogLevel, 'LOG_LEVEL', LogLevel.INFO)
        environment = get_enum_from_env(Environment, 'ENV', Environment.DEVELOPMENT)
        
        # Using with Kubernetes ConfigMaps
        # In your ConfigMap:
        # apiVersion: v1
        # kind: ConfigMap
        # metadata:
        #   name: app-config
        # data:
        #   USER_ROLE: "ROLE_ADMIN"
        #   LOG_LEVEL: "DEBUG"
        
        # Then in your application:
        user_role = get_enum_from_env(UserRole, 'USER_ROLE', UserRole.USER)
        log_level = get_enum_from_env(LogLevel, 'LOG_LEVEL', LogLevel.INFO)
        
        # Using with feature flags
        class FeatureFlag(Enum):
            ENABLED = 'enabled'
            DISABLED = 'disabled'
            BETA = 'beta'
            
        feature_status = get_enum_from_env(FeatureFlag, 'FEATURE_STATUS', FeatureFlag.DISABLED)
        if feature_status == FeatureFlag.ENABLED:
            # Enable feature
            pass
    """
    env_value = os.getenv(env_var_name, default_value.value)
    logger.debug(
        "Attempting to retrieve %s from environment: %s",
        env_var_name,
        env_value
    )
    try:
        # Try to find the enum member by value
        for member in enum_type:
            if member.value == env_value:
                logger.debug(
                    "%s set to %s from environment.",
                    env_var_name,
                    member.value
                )
                return member
        # If not found by value, try by name (case-insensitive)
        enum_member = enum_type[env_value.upper()]
        logger.debug(
            "%s set to %s from environment.",
            env_var_name,
            enum_member.value
        )
        return enum_member
    except KeyError:
        logger.warning(
            "Invalid value %s for %s. Using default: %s",
            env_value,
            env_var_name,
            default_value.value
        )
        return default_value


def get_bool_from_env(
        env_var_name: str,
        default_value: bool
) -> bool:
    """Retrieves a boolean value from an environment variable.

    This helper function attempts to retrieve a boolean value from the
    specified environment variable. If the environment variable is not set
    or the value is invalid, it returns the provided default value.

    The function recognizes various string representations of boolean values:
    - True values: 'true', 'True', 'TRUE', '1', 'yes', 'YES', 'y'
    - False values: 'false', 'False', 'FALSE', '0', 'no', 'NO', 'n'

    Args:
        env_var_name (str): The name of the environment variable to retrieve.
        default_value (bool): The default boolean value to return if the
        environment variable is not set or the value is invalid.

    Returns:
        bool: The boolean value corresponding to the environment variable
        value, or the default value if the environment variable is not set
        or the value is invalid.
        
    Usage:
        # Basic usage with default value
        debug_mode = get_bool_from_env('DEBUG_MODE', default_value=False)
        # If DEBUG_MODE=1, debug_mode will be True
        # If DEBUG_MODE=0, debug_mode will be False
        # If DEBUG_MODE is not set, debug_mode will be False
        
        # Using in a configuration class
        class AppConfig:
            def __init__(self):
                self.enable_logging = get_bool_from_env('ENABLE_LOGGING', True)
                self.verbose_output = get_bool_from_env('VERBOSE_OUTPUT', False)
                self.allow_anonymous = get_bool_from_env('ALLOW_ANONYMOUS', False)
                
        # Using in a FastAPI application
        from fastapi import FastAPI
        from cba_core_lib.utils.env_utils import get_bool_from_env
        
        app = FastAPI()
        
        # Configure application based on environment variables
        app.debug = get_bool_from_env('APP_DEBUG', False)
        app.testing = get_bool_from_env('APP_TESTING', False)
        
        # Using in a Docker environment
        # In your Dockerfile or docker-compose.yml:
        # environment:
        #   - ENABLE_CACHING=true
        #   - DEBUG_MODE=1
        #   - ALLOW_ANONYMOUS=false
        
        # Then in your application:
        enable_caching = get_bool_from_env('ENABLE_CACHING', True)
        debug_mode = get_bool_from_env('DEBUG_MODE', False)
        allow_anonymous = get_bool_from_env('ALLOW_ANONYMOUS', False)
        
        # Using with Kubernetes ConfigMaps
        # In your ConfigMap:
        # apiVersion: v1
        # kind: ConfigMap
        # metadata:
        #   name: app-config
        # data:
        #   ENABLE_METRICS: "true"
        #   DEBUG_MODE: "1"
        
        # Then in your application:
        enable_metrics = get_bool_from_env('ENABLE_METRICS', False)
        debug_mode = get_bool_from_env('DEBUG_MODE', False)
    """
    env_value = os.getenv(env_var_name, str(default_value)).lower()
    logger.debug(
        "Attempting to retrieve %s from environment: %s",
        env_var_name, env_value
    )

    true_values = {'true', '1', 'yes', 'y'}
    false_values = {'false', '0', 'no', 'n'}

    if env_value in true_values:
        logger.debug("%s set to True from environment.", env_var_name)
        return True

    if env_value in false_values:
        logger.debug("%s set to False from environment.", env_var_name)
        return False

    logger.warning(
        "Invalid boolean value '%s' for %s. Using default: %s",
        env_value,
        env_var_name,
        default_value,
    )

    return default_value


def get_int_from_env(
        env_var_name: str,
        default_value: int
) -> int:
    """Retrieves an integer value from an environment variable.

    This helper function attempts to retrieve an integer value from the specified
    environment variable. If the environment variable is not set or the value is
    invalid, it returns the provided default value.

    Args:
        env_var_name (str): The name of the environment variable to retrieve.
        default_value (int): The default integer value to return if the environment
            variable is not set or the value is invalid.

    Returns:
        int: The integer value corresponding to the environment variable value, or
            the default value if the environment variable is not set or the value is
            invalid.
            
    Usage:
        # Basic usage with default value
        port = get_int_from_env('PORT', default_value=8000)
        # If PORT=8080, port will be 8080
        # If PORT is not set, port will be 8000
        # If PORT=invalid, port will be 8000
        
        # Using in a configuration class
        class AppConfig:
            def __init__(self):
                self.max_connections = get_int_from_env('MAX_CONNECTIONS', 100)
                self.timeout_seconds = get_int_from_env('TIMEOUT_SECONDS', 30)
                self.retry_attempts = get_int_from_env('RETRY_ATTEMPTS', 3)
                
        # Using in a FastAPI application
        from fastapi import FastAPI
        from cba_core_lib.utils.env_utils import get_int_from_env
        
        app = FastAPI()
        
        # Configure application based on environment variables
        app.port = get_int_from_env('APP_PORT', 8000)
        app.workers = get_int_from_env('APP_WORKERS', 4)
        
        # Using in a Docker environment
        # In your Dockerfile or docker-compose.yml:
        # environment:
        #   - MAX_CONNECTIONS=200
        #   - TIMEOUT_SECONDS=60
        #   - RETRY_ATTEMPTS=5
        
        # Then in your application:
        max_connections = get_int_from_env('MAX_CONNECTIONS', 100)
        timeout_seconds = get_int_from_env('TIMEOUT_SECONDS', 30)
        retry_attempts = get_int_from_env('RETRY_ATTEMPTS', 3)
        
        # Using with Kubernetes ConfigMaps
        # In your ConfigMap:
        # apiVersion: v1
        # kind: ConfigMap
        # metadata:
        #   name: app-config
        # data:
        #   MAX_CONNECTIONS: "200"
        #   TIMEOUT_SECONDS: "60"
        
        # Then in your application:
        max_connections = get_int_from_env('MAX_CONNECTIONS', 100)
        timeout_seconds = get_int_from_env('TIMEOUT_SECONDS', 30)
        
        # Using with database configuration
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': get_int_from_env('DB_PORT', 5432),
            'pool_size': get_int_from_env('DB_POOL_SIZE', 20),
            'max_overflow': get_int_from_env('DB_MAX_OVERFLOW', 10)
        }
    """
    env_value = os.getenv(env_var_name)
    if env_value is None:
        logger.debug(
            "%s not set, using default value: %s",
            env_var_name,
            default_value
        )
        return default_value

    try:
        int_value = int(env_value)
        logger.debug(
            "%s set to: %s",
            env_var_name,
            int_value
        )
        return int_value
    except ValueError:
        logger.warning(
            "Invalid integer value %s for %s. Using default: %s",
            env_value,
            env_var_name,
            default_value
        )
        return default_value
