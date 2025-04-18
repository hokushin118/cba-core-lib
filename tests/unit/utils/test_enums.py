"""
Enums Unit Test Suite.

Test cases can be run with the following:
  pytest -v --cov=cba_core_lib --cov-report=term-missing --cov-branch
"""

import pytest

from cba_core_lib.utils.enums import UserRole, Status


######################################################################
#  ENUMS UNIT TEST CASES
######################################################################
class TestUserRoleEnum:
    """UserRole Enum Tests."""

    def test_user_role_values(self):
        """It should have the correct string values for each enum member."""
        assert UserRole.USER.value == 'ROLE_USER'
        assert UserRole.MODERATOR.value == 'ROLE_MODERATOR'
        assert UserRole.ADMIN.value == 'ROLE_ADMIN'

    def test_user_role_role_value_property(self):
        """It should return the correct string from the role_value property."""
        assert UserRole.USER.role_value == 'ROLE_USER'
        assert UserRole.MODERATOR.role_value == 'ROLE_MODERATOR'
        assert UserRole.ADMIN.role_value == 'ROLE_ADMIN'

    def test_user_role_enum_creation_from_value(self):
        """It should create enum members from their string values."""
        assert UserRole('ROLE_USER') == UserRole.USER
        assert UserRole('ROLE_MODERATOR') == UserRole.MODERATOR
        assert UserRole('ROLE_ADMIN') == UserRole.ADMIN

    def test_user_role_enum_comparison(self):
        """It should allow comparison of enum members."""
        assert UserRole.USER == UserRole.USER
        assert UserRole.USER != UserRole.MODERATOR
        assert UserRole.MODERATOR != UserRole.ADMIN

    def test_user_role_name_attribute(self):
        """It should return the correct name attribute of enum members."""
        assert UserRole.USER.name == 'USER'
        assert UserRole.MODERATOR.name == 'MODERATOR'
        assert UserRole.ADMIN.name == 'ADMIN'

    def test_user_role_invalid_value(self):
        """It should raise ValueError when creating an enum member with
        an invalid string."""
        with pytest.raises(ValueError):
            UserRole('INVALID_ROLE')


class TestStatusEnum:
    """Status Enum Tests."""

    def test_status_values(self):
        """It should have the correct string values for each enum member."""
        assert Status.SUCCESS.value == 'SUCCESS'
        assert Status.FAILURE.value == 'FAILURE'
        assert Status.PENDING.value == 'PENDING'
        assert Status.RUNNING.value == 'RUNNING'
        assert Status.COMPLETED.value == 'COMPLETED'

    def test_status_status_value_property(self):
        """It should return the correct string from the status_value
        property."""
        assert Status.SUCCESS.status_value == 'SUCCESS'
        assert Status.FAILURE.status_value == 'FAILURE'
        assert Status.PENDING.status_value == 'PENDING'
        assert Status.RUNNING.status_value == 'RUNNING'
        assert Status.COMPLETED.status_value == 'COMPLETED'

    def test_status_enum_creation_from_value(self):
        """It should create enum members from their string values."""
        assert Status('SUCCESS') == Status.SUCCESS
        assert Status('FAILURE') == Status.FAILURE
        assert Status('PENDING') == Status.PENDING
        assert Status('RUNNING') == Status.RUNNING
        assert Status('COMPLETED') == Status.COMPLETED

    def test_status_enum_comparison(self):
        """It should allow comparison of enum members."""
        assert Status.SUCCESS == Status.SUCCESS
        assert Status.SUCCESS != Status.FAILURE
        assert Status.FAILURE == Status.FAILURE
        assert Status.FAILURE != Status.PENDING
        assert Status.PENDING == Status.PENDING
        assert Status.PENDING != Status.RUNNING
        assert Status.RUNNING == Status.RUNNING
        assert Status.COMPLETED != Status.SUCCESS
        assert Status.COMPLETED == Status.COMPLETED

    def test_status_name_attribute(self):
        """It should return the correct name attribute of enum members."""
        assert Status.SUCCESS.name == 'SUCCESS'
        assert Status.FAILURE.name == 'FAILURE'
        assert Status.PENDING.name == 'PENDING'
        assert Status.RUNNING.name == 'RUNNING'
        assert Status.COMPLETED.name == 'COMPLETED'

    def test_status_invalid_value(self):
        """It should raise ValueError when creating an enum member with
        an invalid string."""
        with pytest.raises(ValueError):
            Status('INVALID_STATUS')
