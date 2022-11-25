import os
import uuid
from dataclasses import dataclass
from typing import Optional

from common import FREE_STORAGE, WALLET_PASS
from neofs_testlib.shell import Shell
from neofs_testlib.utils.wallet import get_last_address_from_wallet, init_wallet
from python_keywords.payment_neogo import deposit_gas, transfer_gas


@dataclass
class WalletFile:
    path: str
    password: str

    def get_address(self) -> str:
        """
        Extracts the last address from wallet.

        Returns:
            The address of the wallet.
        """
        return get_last_address_from_wallet(self.path, self.password)


class WalletFactory:
    def __init__(self, wallets_dir: str, shell: Shell) -> None:
        self.shell = shell
        self.wallets_dir = wallets_dir

    def create_wallet(self, password: str = WALLET_PASS) -> WalletFile:
        """
        Creates new default wallet
        Args:
            password: wallet password

        Returns:
            WalletFile object of new wallet
        """
        wallet_path = os.path.join(self.wallets_dir, f"{str(uuid.uuid4())}.json")
        init_wallet(wallet_path, password)
        if not FREE_STORAGE:
            deposit = 30
            transfer_gas(
                shell=self.shell,
                amount=deposit + 1,
                wallet_to_path=wallet_path,
                wallet_to_password=password,
            )
            deposit_gas(
                shell=self.shell,
                amount=deposit,
                wallet_from_path=wallet_path,
                wallet_from_password=password,
            )

        return WalletFile(wallet_path, password)
