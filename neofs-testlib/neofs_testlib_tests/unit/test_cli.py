from unittest import TestCase
from unittest.mock import Mock

from neofs_testlib.cli import NeofsAdm, NeofsCli, NeoGo
from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell.interfaces import CommandOptions, InteractiveInput


class TestCli(TestCase):
    neofs_adm_exec_path = "neo-adm-exec"
    neofs_go_exec_path = "neo-go-exec"
    neofs_cli_exec_path = "neo-cli-exec"

    address = "0x0000000000000000000"
    addresses = ["0x000000", "0xDEADBEEF", "0xBABECAFE"]
    amount = 100
    file1 = "file_1"
    file2 = "directory/file_2"
    manifest = "manifest1"
    token = "GAS"
    rpc_endpoint = "endpoint-1"
    sysgas: float = 0.001
    wallet = "wallet1"
    wallet_password = "P@$$w0rd"
    config_file = "config.yml"
    basic_acl = "1FBFBFFF"
    policy = "policy1"
    timeout = 20
    xhdr = {"param1": "value1", "param2": "value2"}
    shards_id = ["123", "2", "3"]
    degraded_mode = "degraded - read - only"
    path_to_objects = "path/to/objects"
    shard_id = "123"

    def test_container_create(self):
        shell = Mock()
        neofs_cli = NeofsCli(
            config_file=self.config_file,
            neofs_cli_exec_path=self.neofs_cli_exec_path,
            shell=shell,
        )
        neofs_cli.container.create(
            rpc_endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            basic_acl=self.basic_acl,
            policy=self.policy,
            await_mode=True,
            xhdr=self.xhdr,
        )

        xhdr = ",".join(f"{param}={value}" for param, value in self.xhdr.items())
        expected_command = (
            f"{self.neofs_cli_exec_path} --config {self.config_file} container create "
            f"--rpc-endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' "
            f"--basic-acl '{self.basic_acl}' --await --policy '{self.policy}' "
            f"--xhdr '{xhdr}'"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_bad_wallet_argument(self):
        shell = Mock()
        neo_go = NeoGo(
            shell=shell, config_path=self.config_file, neo_go_exec_path=self.neofs_go_exec_path
        )
        with self.assertRaises(Exception) as exc_msg:
            neo_go.contract.add_group(
                address=self.address,
                manifest=self.manifest,
                wallet_password=self.wallet_password,
            )
        self.assertEqual(CliCommand.WALLET_SOURCE_ERROR_MSG, str(exc_msg.exception))

        with self.assertRaises(Exception) as exc_msg:
            neo_go.contract.add_group(
                wallet=self.wallet,
                wallet_password=self.wallet_password,
                wallet_config=self.config_file,
                address=self.address,
                manifest=self.manifest,
            )
        self.assertEqual(CliCommand.WALLET_SOURCE_ERROR_MSG, str(exc_msg.exception))

        with self.assertRaises(Exception) as exc_msg:
            neo_go.contract.add_group(
                wallet=self.wallet,
                address=self.address,
                manifest=self.manifest,
            )
        self.assertEqual(CliCommand.WALLET_PASSWD_ERROR_MSG, str(exc_msg.exception))

    def test_wallet_sign(self):
        shell = Mock()
        neo_go = NeoGo(
            shell=shell, config_path=self.config_file, neo_go_exec_path=self.neofs_go_exec_path
        )
        neo_go.wallet.sign(
            input_file=self.file1,
            out=self.file2,
            rpc_endpoint=self.rpc_endpoint,
            address=self.address,
            wallet=self.wallet,
            wallet_password=self.wallet_password,
            timeout=self.timeout,
        )

        expected_command = (
            f"{self.neofs_go_exec_path} --config_path {self.config_file} wallet sign "
            f"--input-file '{self.file1}' --address '{self.address}' "
            f"--rpc-endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' "
            f"--out '{self.file2}' --timeout '{self.timeout}s'"
        )

        shell.exec.assert_called_once_with(
            expected_command,
            options=CommandOptions(
                interactive_inputs=[
                    InteractiveInput(prompt_pattern="assword", input=self.wallet_password)
                ]
            ),
        )

    def test_subnet_create(self):
        shell = Mock()
        neofs_adm = NeofsAdm(
            config_file=self.config_file,
            neofs_adm_exec_path=self.neofs_adm_exec_path,
            shell=shell,
        )
        neofs_adm.subnet.create(
            address=self.address,
            rpc_endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            notary=True,
        )

        expected_command = (
            f"{self.neofs_adm_exec_path} --config {self.config_file} morph subnet create "
            f"--rpc-endpoint '{self.rpc_endpoint}' --address '{self.address}' "
            f"--wallet '{self.wallet}' --notary"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_wallet_nep17_multitransfer(self):
        shell = Mock()
        neo_go = NeoGo(
            shell=shell, config_path=self.config_file, neo_go_exec_path=self.neofs_go_exec_path
        )
        neo_go.nep17.multitransfer(
            wallet=self.wallet,
            token=self.token,
            to_address=self.addresses,
            sysgas=self.sysgas,
            rpc_endpoint=self.rpc_endpoint,
            amount=self.amount,
            force=True,
            from_address=self.address,
            timeout=self.timeout,
        )

        to_address = "".join(f" --to '{address}'" for address in self.addresses)
        expected_command = (
            f"{self.neofs_go_exec_path} --config_path {self.config_file} "
            f"wallet nep17 multitransfer --token '{self.token}'"
            f"{to_address} --sysgas '{self.sysgas}' --rpc-endpoint '{self.rpc_endpoint}' "
            f"--wallet '{self.wallet}' --from '{self.address}' --force --amount {self.amount} "
            f"--timeout '{self.timeout}s'"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_version(self):
        shell = Mock()
        neofs_adm = NeofsAdm(shell=shell, neofs_adm_exec_path=self.neofs_adm_exec_path)
        neofs_adm.version.get()

        shell.exec.assert_called_once_with(f"{self.neofs_adm_exec_path}   --version")

    def test_shards_flush_cache(self):
        shell = Mock()

        neofs_cli = NeofsCli(
            config_file=self.config_file,
            neofs_cli_exec_path=self.neofs_cli_exec_path,
            shell=shell,
        )

        neofs_cli.shards.flush_cache(
            endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            shards_id=self.shards_id,
        )

        expected_command = (
            f"{self.neofs_cli_exec_path} --config {self.config_file} control shards flush-cache "
            f"--endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' "
            f"--id '{self.shards_id[0]}' --id '{self.shards_id[1]}' --id '{self.shards_id[2]}'"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_shards_set_mode(self):
        shell = Mock()

        neofs_cli = NeofsCli(
            config_file=self.config_file,
            neofs_cli_exec_path=self.neofs_cli_exec_path,
            shell=shell,
        )

        neofs_cli.shards.set_mode(
            endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            mode=self.degraded_mode,
            shards_id=self.shards_id,
        )

        expected_command = (
            f"{self.neofs_cli_exec_path} --config {self.config_file} control shards set-mode "
            f"--endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' --mode '{self.degraded_mode}' "
            f"--id '{self.shards_id[0]}' --id '{self.shards_id[1]}' --id '{self.shards_id[2]}'"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_shards_dump(self):
        shell = Mock()

        neofs_cli = NeofsCli(
            config_file=self.config_file,
            neofs_cli_exec_path=self.neofs_cli_exec_path,
            shell=shell,
        )

        neofs_cli.shards.dump(
            endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            path=self.path_to_objects,
            shard_id=self.shard_id,
        )

        expected_command = (
            f"{self.neofs_cli_exec_path} --config {self.config_file} control shards dump "
            f"--endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' --id '{self.shard_id}' "
            f"--path '{self.path_to_objects}'"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_shards_list(self):
        shell = Mock()

        neofs_cli = NeofsCli(
            config_file=self.config_file,
            neofs_cli_exec_path=self.neofs_cli_exec_path,
            shell=shell,
        )

        neofs_cli.shards.list(
            endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            address=self.address,
        )

        expected_command = (
            f"{self.neofs_cli_exec_path} --config {self.config_file} control shards list "
            f"--endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' --address '{self.address}'"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_shards_evacuate(self):
        shell = Mock()

        neofs_cli = NeofsCli(
            config_file=self.config_file,
            neofs_cli_exec_path=self.neofs_cli_exec_path,
            shell=shell,
        )

        neofs_cli.shards.evacuate(
            endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            shards_id=self.shards_id,
        )

        expected_command = (
            f"{self.neofs_cli_exec_path} --config {self.config_file} control shards evacuate "
            f"--endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' "
            f"--id '{self.shards_id[0]}' --id '{self.shards_id[1]}' --id '{self.shards_id[2]}'"
        )

        shell.exec.assert_called_once_with(expected_command)

    def test_shards_restore(self):
        shell = Mock()

        neofs_cli = NeofsCli(
            config_file=self.config_file,
            neofs_cli_exec_path=self.neofs_cli_exec_path,
            shell=shell,
        )

        neofs_cli.shards.restore(
            endpoint=self.rpc_endpoint,
            wallet=self.wallet,
            shard_id=self.shard_id,
            path=self.path_to_objects,
        )

        expected_command = (
            f"{self.neofs_cli_exec_path} --config {self.config_file} control shards restore "
            f"--endpoint '{self.rpc_endpoint}' --wallet '{self.wallet}' --id '{self.shard_id}' "
            f"--path '{self.path_to_objects}'"
        )

        shell.exec.assert_called_once_with(expected_command)
