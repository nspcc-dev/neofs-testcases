#!/usr/bin/python3.8

import pexpect

from robot.api.deco import keyword

ROBOT_AUTO_KEYWORDS = False

@keyword('Run Process And Enter Empty Password')
def run_proccess_and_interact(cmd: str) -> str:
    p = pexpect.spawn(cmd)
    p.expect("[pP]assword")
    # enter empty password
    p.sendline('\r')
    p.wait()
    # throw a string with password prompt
    first = p.readline()
    # take all output
    child_output = p.readline()
    p.close()
    if p.exitstatus != 0:
        raise Exception(f"{first}\n{child_output}")
    return child_output
