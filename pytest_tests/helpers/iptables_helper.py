from ssh_helper import HostClient


# TODO: convert to shell from hosting
class IpTablesHelper:
    @staticmethod
    def drop_input_traffic_to_port(client: HostClient, ports: list[str]):
        for port in ports:
            cmd_output = client.exec(cmd=f"sudo iptables -A INPUT -p tcp --dport {port} -j DROP")
            assert cmd_output.rc == 0

    @staticmethod
    def restore_input_traffic_to_port(client: HostClient, ports: list[str]):
        for port in ports:
            cmd_output = client.exec(cmd=f"sudo iptables -D INPUT -p tcp --dport {port} -j DROP")
            assert cmd_output.rc == 0
