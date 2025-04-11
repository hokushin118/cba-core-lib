"""
Env Utils Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""
import os
from unittest.mock import patch

import pytest
from cba_core_lib.utils.enums import UserRole
from cba_core_lib.utils.env_utils import (
    get_enum_from_env,
    get_bool_from_env,
    get_int_from_env
)


######################################################################
#  ENV UTILS UNIT TEST CASES
######################################################################
@pytest.mark.parametrize(
    'env_value, expected_enum',
    [
        ('ROLE_ADMIN', UserRole.ADMIN),
        ('ROLE_USER', UserRole.USER),
        ('ROLE_MODERATOR', UserRole.MODERATOR),
        # Case-insensitive member name
        ('role_user', UserRole.USER),
    ],
    ids=[
        'valid_role_admin',
        'valid_role_user',
        'mixed_case_role_moderator',
        'lower_case_value'
    ]
)
def test_get_enum_from_env_returns_member(env_value, expected_enum):
    """It should return the correct enum member for valid environment
    variable values."""
    env_var_name = 'TEST_USER_ROLE'
    default_value = UserRole.USER

    env_vars = {env_var_name: env_value} if env_value is not None else {}
    with patch.dict(os.environ, env_vars, clear=True):
        actual = get_enum_from_env(
            UserRole,
            env_var_name,
            default_value
        )
        assert actual == expected_enum


@pytest.mark.parametrize(
    'env_value',
    [
        'invalid',  # Completely invalid value
        '',  # Empty string
        None,  # Represents variable not being set
    ],
    ids=['invalid_string', 'empty_string', 'not_set']
)
def test_get_enum_from_env_returns_default(env_value):
    """It should return the default enum member for invalid or missing
    environment variable values."""
    env_var_name = 'TEST_USER_ROLE'
    default_value = UserRole.USER

    env_vars = {env_var_name: env_value} if env_value is not None else {}
    with patch.dict(os.environ, env_vars, clear=True):
        actual = get_enum_from_env(
            UserRole,
            env_var_name,
            default_value
        )
        assert actual == default_value


@pytest.mark.parametrize(
    'env_value',
    [
        'true',
        'True',
        'TRUE',
        'tRuE',
        '1',
        'yes',
        'YES',
        'y'
    ],
    # Common true values
    ids=[
        'true_lower',
        'true_title',
        'true_upper',
        'true_mixed',
        'one',
        'yes_lower',
        'yes_upper',
        'y'
    ]
)
def test_get_bool_from_env_is_true(env_value):
    """It should return True for various 'true' string representations."""
    env_var_name = 'TEST_BOOL_VAR'
    default_value = False
    with patch.dict(os.environ, {env_var_name: env_value}, clear=True):
        assert get_bool_from_env(
            env_var_name,
            default_value
        ) is True


@pytest.mark.parametrize(
    'env_value',
    [
        'false',
        'False',
        'FALSE',
        'fAlSe',
        '0',
        'no',
        'NO',
        'n'
    ],
    # Common false values
    ids=[
        'false_lower',
        'false_title',
        'false_upper',
        'false_mixed',
        'zero',
        'no_lower',
        'no_upper',
        'n'
    ]
)
def test_get_bool_from_env_is_false(env_value):
    """It should return False for various 'false' string representations."""
    env_var_name = 'TEST_BOOL_VAR'
    default_value = True
    with patch.dict(os.environ, {env_var_name: env_value}, clear=True):
        assert get_bool_from_env(
            env_var_name,
            default_value
        ) is False


@pytest.mark.parametrize(
    'env_value, default_value, expected_result',
    [
        ('invalid', True, True),  # Invalid string, default True
        ('invalid', False, False),  # Invalid string, default False
        ('', True, True),  # Empty string, default True
        ('', False, False),  # Empty string, default False
        (None, True, True),  # Not set, default True
        (None, False, False),  # Not set, default False
    ],
    ids=[
        'invalid_def_true', 'invalid_def_false',
        'empty_def_true', 'empty_def_false',
        'not_set_def_true', 'not_set_def_false'
    ]
)
def test_get_bool_from_env_uses_default(
        env_value,
        default_value,
        expected_result
):
    """It should return the correct default boolean value for invalid or
     missing environment values."""
    env_var_name = 'TEST_BOOL_VAR'
    env_vars = {env_var_name: env_value} if env_value is not None else {}
    with patch.dict(os.environ, env_vars, clear=True):
        assert get_bool_from_env(
            env_var_name,
            default_value
        ) is expected_result


@pytest.mark.parametrize(
    'env_value, expected_int',
    [
        ('5', 5),
        ('0', 0),
        ('-50', -50),
        ('1000000', 1000000),
    ],
    ids=[
        'positive',
        'zero',
        'negative',
        'large'
    ]
)
def test_get_int_from_env_parses_correctly(
        env_value,
        expected_int
):
    """It should parse valid integer strings correctly."""
    env_var_name = 'TEST_INT_VAR'
    default_value = 999
    with patch.dict(os.environ, {env_var_name: env_value}, clear=True):
        assert get_int_from_env(
            env_var_name,
            default_value
        ) == expected_int


@pytest.mark.parametrize(
    'env_value, default_value',
    [
        ('invalid', 5),  # Invalid string
        ('', 10),  # Empty string
        ('10.5', 3),  # Float string
        (None, -1),  # Not set
    ],
    ids=[
        'invalid_str',
        'empty_str',
        'float_str',
        'not_set'
    ]
)
def test_get_int_from_env_uses_default(
        env_value,
        default_value
):
    """It should return the default integer value for invalid or missing
    environment values."""
    env_var_name = 'TEST_INT_VAR'
    env_vars = {env_var_name: env_value} if env_value is not None else {}
    with patch.dict(os.environ, env_vars, clear=True):
        assert get_int_from_env(
            env_var_name,
            default_value
        ) == default_value
