import tempfile
import unittest
from pathlib import Path

from auth_manager import AuthError, AuthManager, DEFAULT_ADMIN_PHONE


class AuthManagerTests(unittest.TestCase):
    def make_manager(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        return AuthManager(str(Path(tmp.name) / "pro_image.db"))

    def test_default_admin_phone_is_seeded_without_password(self):
        auth = self.make_manager()

        user = auth.get_user_by_phone(DEFAULT_ADMIN_PHONE)

        self.assertIsNotNone(user)
        self.assertEqual("admin", user["role"])
        self.assertFalse(user["has_password"])

    def test_only_preapproved_phone_can_register_then_login(self):
        auth = self.make_manager()

        with self.assertRaises(AuthError) as err:
            auth.register("13800000000", "first-pass")
        self.assertEqual("phone_not_allowed", err.exception.code)

        admin = auth.get_user_by_phone(DEFAULT_ADMIN_PHONE)
        auth.add_allowed_user(admin, "13800000000", "member")

        user = auth.register("13800000000", "first-pass")
        self.assertEqual("member", user["role"])
        self.assertTrue(auth.get_user_by_phone("13800000000")["has_password"])

        logged_in = auth.authenticate("13800000000", "first-pass")
        self.assertEqual(user["id"], logged_in["id"])

        with self.assertRaises(AuthError) as repeat_err:
            auth.register("13800000000", "other-pass")
        self.assertEqual("already_registered", repeat_err.exception.code)

    def test_admin_and_manager_can_add_allowed_users(self):
        auth = self.make_manager()
        admin = auth.get_user_by_phone(DEFAULT_ADMIN_PHONE)

        manager = auth.add_allowed_user(admin, "13900000000", "manager")
        member = auth.add_allowed_user(manager, "13700000000", "member")

        self.assertEqual("manager", manager["role"])
        self.assertEqual("member", member["role"])

        with self.assertRaises(AuthError) as err:
            auth.add_allowed_user(member, "13600000000", "member")
        self.assertEqual("permission_denied", err.exception.code)

    def test_manager_cannot_create_admin_or_manager_accounts(self):
        auth = self.make_manager()
        admin = auth.get_user_by_phone(DEFAULT_ADMIN_PHONE)
        manager = auth.add_allowed_user(admin, "13900000000", "manager")

        with self.assertRaises(AuthError) as admin_err:
            auth.add_allowed_user(manager, "13600000000", "admin")
        self.assertEqual("permission_denied", admin_err.exception.code)

        with self.assertRaises(AuthError) as manager_err:
            auth.add_allowed_user(manager, "13700000000", "manager")
        self.assertEqual("permission_denied", manager_err.exception.code)

    def test_manager_cannot_change_existing_privileged_accounts(self):
        auth = self.make_manager()
        admin = auth.get_user_by_phone(DEFAULT_ADMIN_PHONE)
        manager = auth.add_allowed_user(admin, "13900000000", "manager")

        with self.assertRaises(AuthError) as err:
            auth.add_allowed_user(manager, DEFAULT_ADMIN_PHONE, "member")
        self.assertEqual("permission_denied", err.exception.code)
        self.assertEqual("admin", auth.get_user_by_phone(DEFAULT_ADMIN_PHONE)["role"])


if __name__ == "__main__":
    unittest.main()
