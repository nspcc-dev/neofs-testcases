import logging

import allure
import neofs_env.neofs_epoch as neofs_epoch
import pytest
from helpers.complex_object_actions import get_nodes_with_object
from helpers.container import list_containers, store_objects
from helpers.file_helper import generate_file
from helpers.grpc_responses import OBJECT_NOT_FOUND
from helpers.neofs_verbs import delete_object, get_object, put_object
from helpers.object_access import read_object
from helpers.test_control import wait_for_success
from helpers.utility import parse_load_report
from helpers.wallet_helpers import create_wallet, get_neofs_balance, mint_neofs_balance
from neofs_testlib.env.env import NeoFSEnv, NodeWallet

logger = logging.getLogger("NeoLogger")

BASIC_INCOME_RATE = 10**16
POOR_BALANCE = "0.1"
RICH_BALANCE = "100"
EPOCH_DURATION = 30
OBJECT_SIZE = 8 * 1024
OBJECTS_IN_CONTAINER = 4

REP_1 = "REP 1 IN X CBF 1 SELECT 1 FROM * AS X"
REP_2 = "REP 2 IN X CBF 1 SELECT 2 FROM * AS X"


@allure.step("Wait until storage nodes have reported the container")
@wait_for_success(90, 5)
def _wait_until_reported(neofs_env: NeoFSEnv, cid: str) -> None:
    report = neofs_env.neofs_adm().fschain.load_report(rpc_endpoint=f"http://{neofs_env.fschain_rpc}", cid=cid).stdout
    entries = parse_load_report(report)
    assert any(objects > 0 and size > 0 for _, objects, size in entries), report


@allure.step("Wait until settlement marks the container unpaid")
@wait_for_success(150, 5)
def _wait_until_unpaid(neofs_env: NeoFSEnv, cid: str) -> int:
    unpaid_since = neofs_env.container_unpaid_since(cid)
    assert unpaid_since is not None, f"{cid} is still paid"
    return unpaid_since


@allure.step("Wait until settlement marks the container paid")
@wait_for_success(90, 5)
def _wait_until_paid(neofs_env: NeoFSEnv, cid: str) -> None:
    assert neofs_env.container_unpaid_since(cid) is None, f"{cid} is still unpaid"


@allure.step("Wait until every storage node rejects PUT")
@wait_for_success(60, 3)
def _wait_until_puts_rejected(neofs_env: NeoFSEnv, wallet: NodeWallet, cid: str, unpaid_since: int) -> None:
    accepted = []
    for node in neofs_env.storage_nodes:
        try:
            oid = put_object(
                wallet.path,
                generate_file(OBJECT_SIZE),
                cid,
                neofs_env.shell,
                node.rpc_endpoint,
                wallet_config=wallet.cli_config,
                timeout="30s",
            )
        except Exception as exc:
            assert f"container is unpaid since epoch {unpaid_since}" in str(exc), exc
        else:
            accepted.append(f"{node.rpc_endpoint} stored {oid}")
    assert not accepted, "storage nodes accepted an object into an unpaid container:\n" + "\n".join(accepted)


def _assert_delete_rejected(neofs_env: NeoFSEnv, wallet: NodeWallet, cid: str, oid: str, unpaid_since: int) -> None:
    with pytest.raises(Exception, match=f"container is unpaid since epoch {unpaid_since}"):
        delete_object(
            wallet.path,
            cid,
            oid,
            neofs_env.shell,
            neofs_env.sn_rpc,
            wallet_config=wallet.cli_config,
        )


@allure.step("Wait until the object can no longer be read")
@wait_for_success(60, 2)
def _wait_until_object_removed(neofs_env: NeoFSEnv, wallet: NodeWallet, cid: str, oid: str) -> None:
    with pytest.raises(Exception, match=OBJECT_NOT_FOUND):
        get_object(
            wallet.path,
            cid,
            oid,
            neofs_env.shell,
            neofs_env.sn_rpc,
            wallet_config=wallet.cli_config,
        )


@allure.step("Wait until the object is GC-marked on the node that stored it")
@wait_for_success(60, 2)
def _wait_until_gc_marked(neofs_env: NeoFSEnv, node, cid: str, oid: str) -> None:
    response = neofs_env.neofs_cli(node.cli_config).control.object_status(
        address=node.wallet.address,
        endpoint=node.control_endpoint,
        object=f"{cid}/{oid}",
        wallet=node.wallet.path,
    )
    assert "GC MARKED" in response.stdout, response.stdout


@allure.step("Wait until the wallet balance decreases")
@wait_for_success(150, 5)
def _wait_until_balance_drops(neofs_env: NeoFSEnv, wallet: NodeWallet, balance_before: float) -> None:
    assert get_neofs_balance(neofs_env, neofs_env.neofs_cli(wallet.cli_config), wallet) < balance_before


@pytest.fixture(scope="module")
def unpaid_env(neofs_env: NeoFSEnv):
    assert int(float(POOR_BALANCE) * 10**12) < BASIC_INCOME_RATE * (16 << 10) // (1 << 30), (
        "poor balance can pay for an object; raise BasicIncomeRate or lower POOR_BALANCE"
    )
    net_info = neofs_env.netmap_netinfo()
    original_rate = net_info["storage_price"]
    original_epoch = net_info["epoch_duration"]
    neofs_env.set_network_config(BasicIncomeRate=BASIC_INCOME_RATE, EpochDuration=EPOCH_DURATION)
    yield neofs_env
    neofs_env.set_network_config(BasicIncomeRate=original_rate, EpochDuration=original_epoch)


@pytest.fixture
def payments_disabled(unpaid_env: NeoFSEnv):
    unpaid_env.set_network_config(BasicIncomeRate=0, EpochDuration=EPOCH_DURATION)
    yield unpaid_env
    unpaid_env.set_network_config(BasicIncomeRate=BASIC_INCOME_RATE, EpochDuration=EPOCH_DURATION)


class TestUnpaidContainer:
    @allure.title("Storage nodes reject new objects when the owner cannot pay for stored data")
    def test_unpaid_container_rejects_new_objects(self, unpaid_env: NeoFSEnv):
        neofs_env = unpaid_env

        with allure.step("Fund one wallet below the per-object charge and one wallet that can pay"):
            poor = create_wallet()
            poor.cli_config = neofs_env.generate_cli_config(poor)
            rich = create_wallet()
            rich.cli_config = neofs_env.generate_cli_config(rich)
            broke = create_wallet()
            broke.cli_config = neofs_env.generate_cli_config(broke)
            poor_balance = mint_neofs_balance(neofs_env, poor, POOR_BALANCE)
            rich_balance = mint_neofs_balance(neofs_env, rich, RICH_BALANCE)
            assert get_neofs_balance(neofs_env, neofs_env.neofs_cli(broke.cli_config), broke) == pytest.approx(0)

        stored: dict[str, tuple[NodeWallet, list[tuple[str, str]]]] = {}
        with allure.step("Store more data than the poor and empty wallets can pay for"):
            for rule in (REP_1, REP_2, "EC 2/1 CBF 1"):
                cid, objects = store_objects(neofs_env, poor, rule, OBJECTS_IN_CONTAINER, OBJECT_SIZE, timeout="30s")
                stored[cid] = (poor, objects)
            broke_cid, broke_objects = store_objects(
                neofs_env, broke, REP_1, OBJECTS_IN_CONTAINER, OBJECT_SIZE, timeout="30s"
            )
            stored[broke_cid] = (broke, broke_objects)

        with allure.step("Store an object for an owner who can pay"):
            rich_cid, rich_objects = store_objects(neofs_env, rich, REP_1, 1, OBJECT_SIZE, timeout="30s")

        with allure.step("Wait until nodes report every container"):
            for cid in list(stored) + [rich_cid]:
                _wait_until_reported(neofs_env, cid)

        unpaid_since: dict[str, int] = {}
        with allure.step("Settlement marks unaffordable containers unpaid, and their objects stay readable"):
            for cid, (wallet, objects) in stored.items():
                unpaid_since[cid] = _wait_until_unpaid(neofs_env, cid)
                logger.info(
                    "Container %s is unpaid since epoch %s (current %s)",
                    cid,
                    unpaid_since[cid],
                    neofs_epoch.get_epoch(neofs_env),
                )
                oid, path = objects[0]
                read_object(neofs_env, wallet, cid, oid, path)

        with allure.step("The funded container is charged and still accepts objects"):
            _wait_until_paid(neofs_env, rich_cid)
            assert get_neofs_balance(neofs_env, neofs_env.neofs_cli(rich.cli_config), rich) < rich_balance
            read_object(neofs_env, rich, rich_cid, rich_objects[0][0], rich_objects[0][1])
            extra = generate_file(OBJECT_SIZE)
            extra_oid = put_object(
                rich.path,
                extra,
                rich_cid,
                neofs_env.shell,
                neofs_env.sn_rpc,
                wallet_config=rich.cli_config,
                timeout="30s",
            )
            read_object(neofs_env, rich, rich_cid, extra_oid, extra)

        with allure.step("Failed settlement does not spend the balance that was too small to pay"):
            assert get_neofs_balance(neofs_env, neofs_env.neofs_cli(poor.cli_config), poor) == pytest.approx(
                poor_balance
            )
            assert get_neofs_balance(neofs_env, neofs_env.neofs_cli(broke.cli_config), broke) == pytest.approx(0)

        with allure.step("Every storage node rejects new objects and tombstones in unpaid containers"):
            for cid, (wallet, objects) in stored.items():
                _wait_until_puts_rejected(neofs_env, wallet, cid, unpaid_since[cid])
                _assert_delete_rejected(neofs_env, wallet, cid, objects[0][0], unpaid_since[cid])

        with allure.step("The same unpaid owner can still write into a new container"):
            fresh_cid, fresh_objects = store_objects(neofs_env, poor, REP_1, 1, OBJECT_SIZE, timeout="30s")
            read_object(neofs_env, poor, fresh_cid, fresh_objects[0][0], fresh_objects[0][1])
            assert neofs_env.container_unpaid_since(fresh_cid) is None

    @allure.title("Topping up the balance restores writes and keeps the stored objects")
    def test_repayment_restores_writes(self, unpaid_env: NeoFSEnv):
        neofs_env = unpaid_env
        wallet = create_wallet()
        wallet.cli_config = neofs_env.generate_cli_config(wallet)
        mint_neofs_balance(neofs_env, wallet, POOR_BALANCE)
        cid, objects = store_objects(neofs_env, wallet, REP_2, OBJECTS_IN_CONTAINER, OBJECT_SIZE, timeout="30s")
        oid, path = objects[0]

        _wait_until_reported(neofs_env, cid)
        _wait_until_unpaid(neofs_env, cid)
        read_object(neofs_env, wallet, cid, oid, path)

        with allure.step("Mint enough GAS for the next settlement"):
            mint_neofs_balance(neofs_env, wallet, RICH_BALANCE)

        _wait_until_paid(neofs_env, cid)
        read_object(neofs_env, wallet, cid, oid, path)
        extra = generate_file(OBJECT_SIZE)
        extra_oid = put_object(
            wallet.path,
            extra,
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
            wallet_config=wallet.cli_config,
            timeout="30s",
        )
        read_object(neofs_env, wallet, cid, extra_oid, extra)

    @allure.title("Unpaid container data is removed after the grace period and writes stay denied")
    def test_unpaid_container_data_is_removed_after_grace_period(self, unpaid_env: NeoFSEnv):
        neofs_env = unpaid_env
        unpaid_epochs_before_removal = 3
        wallet = create_wallet()
        wallet.cli_config = neofs_env.generate_cli_config(wallet)
        mint_neofs_balance(neofs_env, wallet, POOR_BALANCE)
        cid, objects = store_objects(neofs_env, wallet, REP_1, 2, OBJECT_SIZE, timeout="30s")

        _wait_until_reported(neofs_env, cid)
        unpaid_since = _wait_until_unpaid(neofs_env, cid)

        holders = {}
        with allure.step("Objects are readable until the grace period elapses"):
            for oid, path in objects:
                read_object(neofs_env, wallet, cid, oid, path)
                nodes = get_nodes_with_object(cid, oid, neofs_env.shell, neofs_env.storage_nodes, neofs_env)
                assert nodes, f"{cid}/{oid} is not stored on any node"
                holders[oid] = nodes

        removal_epoch = unpaid_since + unpaid_epochs_before_removal
        with allure.step(f"Advance to epoch {removal_epoch}, when unpaid containers are garbage-collected"):
            while neofs_epoch.get_epoch(neofs_env) < removal_epoch:
                neofs_epoch.tick_epoch_and_wait(neofs_env)

        with allure.step("Stored objects are gone, the container remains, and new objects are still rejected"):
            for oid, _ in objects:
                _wait_until_object_removed(neofs_env, wallet, cid, oid)
                for node in holders[oid]:
                    _wait_until_gc_marked(neofs_env, node, cid, oid)
            assert cid in list_containers(wallet.path, neofs_env.shell, neofs_env.sn_rpc)
            _wait_until_puts_rejected(neofs_env, wallet, cid, unpaid_since)

    @allure.title("An owner who can pay for the stored data keeps write access")
    def test_affordable_container_keeps_accepting_objects(self, unpaid_env: NeoFSEnv):
        neofs_env = unpaid_env
        wallet = create_wallet()
        wallet.cli_config = neofs_env.generate_cli_config(wallet)
        balance_before = mint_neofs_balance(neofs_env, wallet, RICH_BALANCE)
        cid, objects = store_objects(neofs_env, wallet, REP_2, OBJECTS_IN_CONTAINER, OBJECT_SIZE, timeout="30s")

        _wait_until_reported(neofs_env, cid)
        with allure.step("Wait until settlement charges the owner and leaves the container paid"):
            _wait_until_balance_drops(neofs_env, wallet, balance_before)
            _wait_until_paid(neofs_env, cid)

        for oid, path in objects:
            read_object(neofs_env, wallet, cid, oid, path)
        extra = generate_file(OBJECT_SIZE)
        extra_oid = put_object(
            wallet.path,
            extra,
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
            wallet_config=wallet.cli_config,
            timeout="30s",
        )
        read_object(neofs_env, wallet, cid, extra_oid, extra)

    @allure.title("A zero basic income rate does not deny puts for a wallet that cannot pay")
    def test_disabled_payments_do_not_reject_puts(self, payments_disabled: NeoFSEnv):
        neofs_env = payments_disabled
        wallet = create_wallet()
        wallet.cli_config = neofs_env.generate_cli_config(wallet)
        mint_neofs_balance(neofs_env, wallet, POOR_BALANCE)
        cid, objects = store_objects(neofs_env, wallet, REP_2, OBJECTS_IN_CONTAINER, OBJECT_SIZE, timeout="30s")
        _wait_until_reported(neofs_env, cid)
        reported_epoch = neofs_epoch.get_epoch(neofs_env)
        neofs_epoch.wait_until_new_epoch(neofs_env, reported_epoch)
        neofs_epoch.wait_until_new_epoch(neofs_env, reported_epoch + 1)
        assert neofs_env.container_unpaid_since(cid) is None
        extra = generate_file(OBJECT_SIZE)
        extra_oid = put_object(
            wallet.path,
            extra,
            cid,
            neofs_env.shell,
            neofs_env.sn_rpc,
            wallet_config=wallet.cli_config,
            timeout="30s",
        )
        read_object(neofs_env, wallet, cid, objects[0][0], objects[0][1])
        read_object(neofs_env, wallet, cid, extra_oid, extra)
