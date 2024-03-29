import json
import os
from unittest import TestCase
from uuid import uuid4

from neo3.wallet import account as neo3_account
from neo3.wallet.wallet import Wallet
from neofs_testlib.utils.wallet import get_last_address_from_wallet, init_wallet


class TestWallet(TestCase):
    DEFAULT_PASSWORD = "password"
    EMPTY_PASSWORD = ""

    def test_init_wallet(self):
        wallet_file_path = f"{str(uuid4())}.json"
        for password in (self.EMPTY_PASSWORD, self.DEFAULT_PASSWORD):
            wrong_password = "wrong_password"
            init_wallet(wallet_file_path, password)
            self.assertTrue(os.path.exists(wallet_file_path))
            with open(wallet_file_path, "r") as wallet_file:
                Wallet.from_json(json.load(wallet_file), passwords=[password])
            with self.assertRaises(ValueError):
                with open(wallet_file_path, "r") as wallet_file:
                    Wallet.from_json(json.load(wallet_file), passwords=[wrong_password])
            os.unlink(wallet_file_path)

    def test_get_last_address_from_wallet(self):
        wallet_file_path = f"{str(uuid4())}.json"
        init_wallet(wallet_file_path, self.DEFAULT_PASSWORD)
        with open(wallet_file_path, "r") as wallet_file:
            wallet = Wallet.from_json(json.load(wallet_file), passwords=[self.DEFAULT_PASSWORD])
        last_address = wallet.accounts[-1].address
        self.assertEqual(
            get_last_address_from_wallet(wallet_file_path, self.DEFAULT_PASSWORD),
            last_address,
        )
        os.unlink(wallet_file_path)

    def test_get_last_address_from_wallet_with_multiple_accounts(self):
        wallet_file_path = f"{str(uuid4())}.json"

        wallet = Wallet()
        account1 = neo3_account.Account.create_new(self.DEFAULT_PASSWORD)
        wallet.account_add(account1)
        account2 = neo3_account.Account.create_new(self.DEFAULT_PASSWORD)
        wallet.account_add(account2)

        with open(wallet_file_path, "w") as out:
            json.dump(wallet.to_json(), out)

        with open(wallet_file_path, "r") as wallet_file:
            wallet = Wallet.from_json(
                json.load(wallet_file), passwords=[self.DEFAULT_PASSWORD, self.DEFAULT_PASSWORD]
            )

        last_address = wallet.accounts[-1].address
        self.assertEqual(
            get_last_address_from_wallet(wallet_file_path, self.DEFAULT_PASSWORD),
            last_address,
        )
        os.unlink(wallet_file_path)
