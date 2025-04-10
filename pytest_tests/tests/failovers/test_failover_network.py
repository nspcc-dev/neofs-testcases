import logging
import subprocess
import sys
from random import choices
from time import sleep

import allure
import pytest
from helpers.complex_object_actions import wait_object_replication
from helpers.container import create_container
from helpers.file_helper import generate_file, get_file_hash
from helpers.iptables_helper import IpTablesHelper
from helpers.neofs_verbs import get_netmap_netinfo, get_object, put_object_to_random_node
from helpers.node_management import storage_node_healthcheck, wait_all_storage_nodes_returned
from helpers.wellknown_acl import PUBLIC_ACL
from neofs_testlib.env.env import NeoFSEnv, StorageNode

logger = logging.getLogger("NeoLogger")
blocked_nodes: list[StorageNode] = []


@pytest.mark.skipif(sys.platform == "darwin", reason="not supported on macos runners")
class TestFailoverNetwork:
    @allure.step("Restore network")
    @pytest.fixture
    def restore_network(self, neofs_env_function_scope: NeoFSEnv):
        self.neofs_env = neofs_env_function_scope
        self.shell = self.neofs_env.shell

        yield

        not_empty = len(blocked_nodes) != 0
        for node in list(blocked_nodes):
            with allure.step(f"Restore network at host for {node}"):
                IpTablesHelper.restore_input_traffic_to_port(self.shell, [node.endpoint.split(":")[1]])
            blocked_nodes.remove(node)
        if not_empty:
            wait_all_storage_nodes_returned(self.neofs_env)

        for node in self.neofs_env.storage_nodes:
            node.kill()
        for node in self.neofs_env.storage_nodes:
            node.start(fresh=False)

        wait_all_storage_nodes_returned(self.neofs_env)

    @allure.title("Block Storage node traffic")
    def test_block_storage_node_traffic(self, default_wallet, restore_network):
        """
        Block storage nodes traffic using iptables and wait for replication for objects.
        """
        wallet = default_wallet
        placement_rule = "REP 2 IN X CBF 2 SELECT 2 FROM * AS X"
        wakeup_node_timeout = 10  # timeout to let nodes detect that traffic has blocked
        nodes_to_block_count = 2

        source_file_path = generate_file(self.neofs_env.get_object_size("simple_object_size"))
        cid = create_container(
            wallet.path,
            shell=self.shell,
            endpoint=self.neofs_env.sn_rpc,
            rule=placement_rule,
            basic_acl=PUBLIC_ACL,
        )
        oid = put_object_to_random_node(wallet.path, source_file_path, cid, shell=self.shell, neofs_env=self.neofs_env)

        nodes = wait_object_replication(
            cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
        )

        logger.info(f"Nodes are {nodes}")
        nodes_to_block = nodes
        if nodes_to_block_count > len(nodes):
            # TODO: the intent of this logic is not clear, need to revisit
            nodes_to_block = choices(nodes, k=2)

        excluded_nodes = []
        for node in nodes_to_block:
            with allure.step(f"Block incoming traffic at node {node} on port {[node.endpoint.split(':')[1]]}"):
                blocked_nodes.append(node)
                excluded_nodes.append(node)
                IpTablesHelper.drop_input_traffic_to_port(self.shell, [node.endpoint.split(":")[1]])
                sleep(wakeup_node_timeout)

            with allure.step(f"Check object is not stored on node {node}"):
                new_nodes = wait_object_replication(
                    cid,
                    oid,
                    2,
                    shell=self.shell,
                    nodes=list(set(self.neofs_env.storage_nodes) - set(excluded_nodes)),
                    neofs_env=self.neofs_env,
                )
                assert node not in new_nodes

            with allure.step("Check object data is not corrupted"):
                got_file_path = get_object(wallet.path, cid, oid, endpoint=new_nodes[0].endpoint, shell=self.shell)
                assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

        for node in nodes_to_block:
            with allure.step(f"Unblock incoming traffic at host {node} on port {[node.endpoint.split(':')[1]]}"):
                IpTablesHelper.restore_input_traffic_to_port(self.shell, [node.endpoint.split(":")[1]])
                blocked_nodes.remove(node)
                sleep(wakeup_node_timeout)

        with allure.step("Check object data is not corrupted"):
            new_nodes = wait_object_replication(
                cid, oid, 2, shell=self.shell, nodes=self.neofs_env.storage_nodes, neofs_env=self.neofs_env
            )

            got_file_path = get_object(wallet.path, cid, oid, shell=self.shell, endpoint=new_nodes[0].endpoint)
            assert get_file_hash(source_file_path) == get_file_hash(got_file_path)

    @pytest.mark.sanity
    @allure.title("RPC reconnection test")
    def test_rpc_reconnection(self, default_wallet, restore_network):
        """
        When RPC connection fails (and it can), storage node reconnects to some other node and continues to operate.
        """
        dport_repeat = 10  # Constant for the number of the disconnect should be repeated

        required_keys = [
            "epoch",
            "time_per_block",
            "audit_fee",
            "storage_price",
            "container_fee",
            "eigentrust_alpha",
            "number_of_eigentrust_iterations",
            "epoch_duration",
            "inner_ring_candidate_fee",
            "maximum_object_size",
            "withdrawal_fee",
            "homomorphic_hashing_disabled",
            "maintenance_mode_allowed",
        ]

        for storage_node in self.neofs_env.storage_nodes:
            pid = storage_node.process.pid

            fschain_addr = self.neofs_env.inner_ring_nodes[0].endpoint.split(":")[0]
            fschain_port = self.neofs_env.inner_ring_nodes[0].endpoint.split(":")[1]

            with allure.step(f"Disconnecting storage node {storage_node} from {fschain_addr} {dport_repeat} times"):
                for repeat in range(dport_repeat):
                    with allure.step(f"Disconnect number {repeat}"):
                        try:
                            """
                            Of course, it would be cleaner to use such code:
                            with Namespace(pid, 'net'):
                                subprocess.check_output(['ss', '-K', 'dst', addr, 'dport', port])
                            But it would be required to run the tests from root, which is bad practice.
                            But we face the limitations of the ubuntu-latest runner:
                            And using setfacl is not possible due to GitHub ubuntu-latest runner limitations.
                            """
                            command = f"ss -K dst {fschain_addr} dport {fschain_port}"
                            sudo_command = f"sudo nsenter -t {pid} -n {command}"
                            output = subprocess.check_output(sudo_command, shell=True)
                            logger.info(f"Output of the command {sudo_command}: {output}")
                        except subprocess.CalledProcessError as e:
                            logger.error(
                                f"Error occurred while running command: {sudo_command}. Error message: {str(e)}"
                            )
                            raise
                        finally:
                            # Delay between shutdown attempts, emulates a real disconnection
                            sleep(1)
                    logger.info(f"Disconnected storage node {storage_node} from {fschain_addr} {dport_repeat} times")

            for node in self.neofs_env.storage_nodes:
                with allure.step(f"Checking if node {node} is alive"):
                    try:
                        health_check = storage_node_healthcheck(node)
                        assert health_check.health_status == "READY" and health_check.network_status == "ONLINE"
                    except Exception as err:
                        logger.warning(f"Node {node} is not online:\n{err}")
                        raise AssertionError(
                            f"After the RPC connection failed, the storage node {node} DID NOT reconnect "
                            f"to any other node and FAILED to continue operating. "
                        )

                with allure.step(f"Checking netinfo for node {node}"):
                    try:
                        net_info = get_netmap_netinfo(
                            wallet=default_wallet.path,
                            endpoint=self.neofs_env.sn_rpc,
                            shell=self.shell,
                        )
                        missing_keys = [key for key in required_keys if key not in net_info]
                        if missing_keys:
                            raise AssertionError(
                                f"Error occurred while checking netinfo for node {node} - "
                                f"missing keys in the output: {missing_keys}."
                                f"Netmap netinfo: {net_info}"
                            )
                    except Exception as err:
                        logger.warning(
                            f"Error occurred while checking netinfo for node {node}. Error message: {str(err)}"
                        )
                        raise Exception(
                            f"After the RPC connection failed, the storage node {node} cannot get netmap netinfo"
                        )

            logger.info(f"Node {node} is alive and online")
