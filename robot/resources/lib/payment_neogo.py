#!/usr/bin/python3

import subprocess
import pexpect
import re
import uuid

from robot.api.deco import keyword
from robot.api import logger

import logging
import robot.errors
import requests
import json

from robot.libraries.BuiltIn import BuiltIn
from neocore.KeyPair import KeyPair

from Crypto import Random

ROBOT_AUTO_KEYWORDS = False



NEOFS_CONTRACT = "5f490fbd8010fd716754073ee960067d28549b7d"
NEOGO_CLI_PREFIX = "docker exec -it main_chain neo-go"
NEO_MAINNET_ENDPOINT = "main_chain.neofs.devenv:30333"

@keyword('Init wallet')
def init_wallet():

    filename = "wallets/" + str(uuid.uuid4()) + ".json"
    cmd = ( f"{NEOGO_CLI_PREFIX} wallet init -w {filename}" )

    logger.info(f"Executing shell command: {cmd}")
    out = run_sh(cmd) 
    logger.info(f"Command completed with output: {out}")
    return filename

@keyword('Generate wallet')
def generate_wallet(wallet: str):
    cmd = ( f"{NEOGO_CLI_PREFIX} wallet create -w {wallet}" )

    logger.info(f"Executing command: {cmd}")
    p = pexpect.spawn(cmd)
    p.expect(".*")
    p.sendline('\n')
    p.sendline('\n')
    p.sendline('\n')
    p.wait()
    out = p.read()

    logger.info(f"Command completed with output: {out}")

@keyword('Dump Address')
def dump_address(wallet: str):
    #"address": "Ngde6LSaBZ58p72trTNkgqEZmX8dTWBgHo",
    address = ""
    cmd = ( f"{NEOGO_CLI_PREFIX} wallet dump -w {wallet}" )

    logger.info(f"Executing command: {cmd}")
    out = run_sh(cmd) 
    logger.info(f"Command completed with output: {out}")

    m = re.search(r'"address": "(\w+)"', out)
    if m.start() != m.end(): 
        address = m.group(1)
    else:
        raise Exception("Can not get address.")

    return address

@keyword('Dump PrivKey')
def dump_privkey(wallet: str, address: str):
    cmd = ( f"{NEOGO_CLI_PREFIX} wallet export -w {wallet} --decrypt {address}" )

    logger.info(f"Executing command: {cmd}")
    out = run_sh_with_passwd('', cmd)
    logger.info(f"Command completed with output: {out}")

    return out


@keyword('Transfer Mainnet Gas') 
# docker cp wallets/wallet.json main_chain:/wallets/

def transfer_mainnet_gas(wallet: str, address: str, address_to: str, amount: int):
    cmd = ( f"{NEOGO_CLI_PREFIX} wallet nep5 transfer -w {wallet} -r http://main_chain.neofs.devenv:30333 --from {address} "
            f"--to {address_to} --token gas --amount {amount}" )

    logger.info(f"Executing command: {cmd}")
    out = run_sh_with_passwd('', cmd)
    logger.info(f"Command completed with output: {out}")

    if not re.match(r'^(\w{64})$', out):
        raise Exception("Can not get Tx.")

    return out

@keyword('Withdraw Mainnet Gas') 
# docker cp wallets/wallet.json main_chain:/wallets/

def withdraw_mainnet_gas(wallet: str, address: str, scripthash: str, amount: int):
    cmd = ( f"{NEOGO_CLI_PREFIX} contract invokefunction -w {wallet} -a {address} -r http://main_chain.neofs.devenv:30333 "
            f"{NEOFS_CONTRACT} withdraw {scripthash} int:{amount}  -- {scripthash}" )

    logger.info(f"Executing command: {cmd}")
    out = run_sh_with_passwd('', cmd)
    logger.info(f"Command completed with output: {out}")

    #if not re.match(r'^(\w{64})$', out):
    #    raise Exception("Can not get Tx.")

    return out

# neo-go contract invokefunction -w wallets/deploy_wallet.json -a NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx -r http://main_chain.neofs.devenv:30333 
# 5f490fbd8010fd716754073ee960067d28549b7d withdraw 12b97a2206ae4b10c7e0194b7b655c32cc912057 int:10  -- 12b97a2206ae4b10c7e0194b7b655c32cc912057


@keyword('Mainnet Balance')
def mainnet_balance(address: str):
    request = 'curl -X POST '+NEO_MAINNET_ENDPOINT+' --cacert ca/nspcc-ca.pem -H \'Content-Type: application/json\' -d \'{ "jsonrpc": "2.0", "id": 5, "method": "getnep5balances", "params": [\"'+address+'\"] }\''
    logger.info(f"Executing request: {request}")

    complProc = subprocess.run(request, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)

    out = complProc.stdout
    logger.info(out)

    m = re.search(r'"668e0c1f9d7b70a99dd9e06eadd4c784d641afbc","amount":"([\d\.]+)"', out)
    if not m.start() != m.end(): 
        raise Exception("Can not get mainnet gas balance.")

    amount = m.group(1)
        
    return amount

@keyword('Expexted Mainnet Balance')
def expected_mainnet_balance(address: str, expected: int):
    
    amount = mainnet_balance(address)

    if float(amount) != float(expected):
        raise Exception(f"Expected amount ({expected}) of GAS has not been found. Found {amount}.")

    return True
# balance":[{"assethash":"668e0c1f9d7b70a99dd9e06eadd4c784d641afbc","amount":"50"
#curl -d '{ "jsonrpc": "2.0", "id": 1, "method": "getnep5balances", "params": ["NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx"] }' main_chain.neofs.devenv:30333
#{"id":1,"jsonrpc":"2.0","result":{"balance":[{"assethash":"668e0c1f9d7b70a99dd9e06eadd4c784d641afbc","amount":"9237.47595500","lastupdatedblock":158}],"address":"NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx"}}




@keyword('NeoFS Deposit') 
def neofs_deposit(wallet: str, address: str, scripthash: str, amount: int):
    cmd = ( f"{NEOGO_CLI_PREFIX} contract invokefunction -w {wallet} -a {address} "
            f"-r http://main_chain.neofs.devenv:30333 {NEOFS_CONTRACT} " 
            f"deposit {scripthash} int:{amount} bytes: -- {scripthash}")

    logger.info(f"Executing command: {cmd}")
    out = run_sh_with_passwd('', cmd)
    logger.info(f"Command completed with output: {out}")

    m = re.match(r'^Sent invocation transaction (\w{64})$', out)
    if m is None:
        raise Exception("Can not get Tx.")

    tx = m.group(1)

    # Sent invocation transaction

    return tx

    #docker exec -it main_chain \
	#	neo-go contract invokefunction \
	#		-w wallets/wallet.json \
	#		-a NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx \
	#		-r http://main_chain.${LOCAL_DOMAIN}:30333 \
	#		${NEOFS_CONTRACT_MAINCHAIN} \
	#		deposit \
	#		12b97a2206ae4b10c7e0194b7b655c32cc912057 \
	#		int:500 \
	#		bytes: \
	#		-- 12b97a2206ae4b10c7e0194b7b655c32cc912057

#neo-go contract invokefunction -w wallets/wallet.json -a NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx 
#-r <http://main_chain.neofs.devenv:30333> af5dc5f7e6a6efc64d679098f328027591a2e518 
#deposit 12b97a2206ae4b10c7e0194b7b655c32cc912057 int:60 bytes: -- 
#12b97a2206ae4b10c7e0194b7b655c32cc912057

 



# wallet nep5 transfer -w wallets/wallet.json -r http://main_chain.neofs.devenv:30333 --from NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx 
# --to NULwe3UAHckN2fzNdcVg31tDiaYtMDwANt --token gas --amount 50

 

@keyword('Transaction accepted in block')
def transaction_accepted_in_block(tx_id):
    """
    This function return True in case of accepted TX.
    Parameters:
    :param tx_id:           transaction is
    :rtype:                 block number or Exception
    """

    logger.info("Transaction id: %s" % tx_id)
    


# curl -d '{ "jsonrpc": "2.0", "id": 1, "method": "getnep5transfers", "params": ["NULwe3UAHckN2fzNdcVg31tDiaYtMDwANt"] }' main_chain.neofs.devenv:30333
    TX_request = 'curl -X POST '+NEO_MAINNET_ENDPOINT+' --cacert ca/nspcc-ca.pem -H \'Content-Type: application/json\' -d \'{ "jsonrpc": "2.0", "id": 5, "method": "gettransactionheight", "params": [\"'+ tx_id +'\"] }\''
    
    logger.info(f"Executing command: {TX_request}")
    

    complProc = subprocess.run(TX_request, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info(complProc.stdout)
    response = json.loads(complProc.stdout)

    if (response['result'] == 0):
        raise Exception( "Transaction is not found in the blocks." )

    logger.info("Transaction has been found in the block %s." % response['result'] )
    return response['result']
    

@keyword('Get Transaction')
def get_transaction(tx_id: str):
    """
    This function return information about TX.
    Parameters:
    :param tx_id:           transaction id
    """

    TX_request = 'curl -X POST '+NEO_MAINNET_ENDPOINT+' --cacert ca/nspcc-ca.pem -H \'Content-Type: application/json\' -d \'{ "jsonrpc": "2.0", "id": 5, "method": "getapplicationlog", "params": [\"'+tx_id+'\"] }\''
    complProc = subprocess.run(TX_request, check=True, universal_newlines=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, shell=True)
    logger.info(complProc.stdout)
    









def run_sh(args):
    complProc = subprocess.run(args, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=150, shell=True)
    output, errors = complProc.stdout, complProc.stderr
    if errors:
        return errors
    return output


def run_sh_with_passwd(passwd, cmd):
    p = pexpect.spawn(cmd)
    p.expect(".*")
    p.sendline(passwd)
    p.wait()
    # throw a string with password prompt
    # take a string with tx hash
    tx_hash = p.read().splitlines()[-1]
    return tx_hash.decode()



#@keyword('Transfer Mainnet Gas')
#def transfer_mainnet_gas(wallet_to: str, amount: int):

#
#    Cmd = f'docker exec -it main_chain neo-go wallet nep5 transfer -w wallets/wallet.json -r http://main_chain.neofs.devenv:30333 --from NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx --to {wallet_to} --token gas --amount {amount}'
#    command = ['docker', 'exec', '-it', 'main_chain', 'neo-go', 'wallet', 'nep5', 'transfer', '-w', 'wallets/wallet.json', '-r', 'http://main_chain.neofs.devenv:30333', '--from NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx', '--to', 'NULwe3UAHckN2fzNdcVg31tDiaYtMDwANt', '--token gas', '--amount', '5']



#    logger.info("Cmd: %s" % Cmd)

#import subprocess
#command = ['myapp', '--arg1', 'value_for_arg1']
#p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#output = p.communicate(input='some data'.encode())[0]

#a=subprocess.Popen("docker run -t -i fedora bash", shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
#4. >>> a.stdin.write("exit\n")
#5. >>> print a.poll()

    complProc = subprocess.Popen(Cmd.split(), stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    complProc.stdin.write("\n".encode())

    output = complProc.stdout.read() #.communicate(input=''.encode())[0]

    logger.info("Output: %s" % output)


#from subprocess import Popen, PIPE
#p = Popen(['python test_enter.py'], stdin=PIPE, shell=True)
#p.communicate(input='\n')



@keyword('Request NeoFS Deposit')
def request_neofs_deposit(public_key: str):
    """
    This function requests Deposit to the selected public key.
    :param public_key:      neo public key
    """

    response = requests.get('https://fs.localtest.nspcc.ru/api/deposit/'+str(public_key), verify='ca/nspcc-ca.pem')  
    
    if response.status_code != 200:
        BuiltIn().fatal_error('Can not run Deposit to {} with error: {}'.format(public_key, response.text))
    else:
        logger.info("Deposit has been completed for '%s'; tx: '%s'" % (public_key, response.text) )

    return response.text

@keyword('Get Balance')
def get_balance(privkey: str):
    """
    This function returns NeoFS balance for selected public key.
    :param public_key:      neo public key
    """

    balance = _get_balance_request(privkey)

    return balance

@keyword('Expected Balance')
def expected_balance(privkey: str, init_amount: float, deposit_size: float):
    """
    This function returns NeoFS balance for selected public key.
    :param public_key:      neo public key
    :param init_amount:     initial number of tokens in the account
    :param deposit_size:    expected amount of the balance increasing
    """

    balance = _get_balance_request(privkey)

    deposit_change = round((float(balance) - init_amount),8)
    if deposit_change != deposit_size:
        raise Exception('Expected deposit increase: {}. This does not correspond to the actual change in account: {}'.format(deposit_size, deposit_change))

    logger.info('Expected deposit increase: {}. This correspond to the actual change in account: {}'.format(deposit_size, deposit_change))

    return deposit_change


def _get_balance_request(privkey: str):
    '''
    Internal method.
    '''
    Cmd = f'neofs-cli --key {privkey} --rpc-endpoint s01.neofs.devenv:8080 accounting balance'
    logger.info("Cmd: %s" % Cmd)
    complProc = subprocess.run(Cmd, check=True, universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=150, shell=True)
    output = complProc.stdout
    logger.info("Output: %s" % output)
    
    
    m = re.match(r'(-?[\d.\.?\d*]+)', output )
    if m is None:
        BuiltIn().fatal_error('Can not parse balance: "%s"' % output)
    balance = m.group(1)

    logger.info("Balance for '%s' is '%s'" % (privkey, balance) )

    return balance

 


 # {"id":5,"jsonrpc":"2.0","result":{"txid":"0x02c178803258a9dbbcce80acfece2f6abb4f51c122e7ce2ddcad332d6a810e5f","trigger":"Application",
 # !!!!!!!!!!!
 #"vmstate":"FAULT"
 # !!!!!!!!!!!
 #,"gasconsumed":"11328110","stack":[],"notifications":[]}}