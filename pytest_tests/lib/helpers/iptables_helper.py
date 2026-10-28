from neofs_testlib.shell import CommandOptions, Shell


class IpTablesHelper:
    @staticmethod
    def drop_input_traffic_to_port(shell: Shell, ports: list[str]) -> None:
        for port in ports:
            shell.exec(f"sudo iptables -A INPUT -p tcp --dport {port} -j DROP")

    @staticmethod
    def restore_input_traffic_to_port(shell: Shell, ports: list[str]) -> None:
        for port in ports:
            rule = f"INPUT -p tcp --dport {port} -j DROP"
            while shell.exec(f"sudo iptables -C {rule}", CommandOptions(check=False)).return_code == 0:
                shell.exec(f"sudo iptables -D {rule}")
