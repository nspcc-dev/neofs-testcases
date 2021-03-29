*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Resource    common_steps_object.robot
 

*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS operations with simple object.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           3 min

    ${WALLET} =         Init wallet
                        Generate wallet         ${WALLET}
    ${ADDR} =           Dump Address            ${WALLET}
    ${PRIV_KEY} =       Dump PrivKey            ${WALLET}               ${ADDR}
    ${TX} =             Transfer Mainnet Gas    wallets/wallet.json     NVUzCUvrbuWadAm6xBoyZ2U7nCmS9QBZtb      ${ADDR}     15
                        Wait Until Keyword Succeeds         1 min       15 sec        
                        ...  Transaction accepted in block  ${TX}
                        Get Transaction                     ${TX}
                        Expected Mainnet Balance            ${ADDR}     15

    [Teardown]          Cleanup                             



*** Keywords ***

Cleanup
                        Cleanup Files                       
                        Get Docker Logs                     object_simple
 




