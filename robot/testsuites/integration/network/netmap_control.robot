*** Settings ***
Variables       common.py

Library         Process
Library         contract_keywords.py
Library         neofs.py
Library         String
Library         acl.py

Resource        setup_teardown.robot
Resource        payment_operations.robot

*** Test Cases ***
Control Operations with storage nodes
    [Documentation]         Testcase to check NetworkInfo control command.
    [Tags]                  NeoFSCLI    NetworkInfo
    [Timeout]               5 min

    [Setup]                 Setup

    ${NODE_NUM}    ${NODE}    ${STORAGE_WALLET} =    Get control endpoint and wallet
    ${empty_list} =         Create List

    ${SNAPSHOT} =           Run Process    ${NEOFS_CLI_EXEC} control netmap-snapshot --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG}    shell=True
    ${HEALTHCHECK} =        Run Process    ${NEOFS_CLI_EXEC} control healthcheck --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG}    shell=True
                            Should Be Equal As Integers    ${HEALTHCHECK.rc}    0

                            Run Process    ${NEOFS_CLI_EXEC} control set-status --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG} --status 'offline'    shell=True
                            
                            Sleep    ${MAINNET_BLOCK_TIME}
                            Tick Epoch

    ${SNAPSHOT_OFFLINE}=    Run Process    ${NEOFS_CLI_EXEC} control netmap-snapshot --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG}    shell=True
    ${NODE_NUM_OFFLINE}=    Get Regexp Matches        ${SNAPSHOT_OFFLINE.stdout}    ${NODE_NUM}
                            Should Be Equal    ${NODE_NUM_OFFLINE}    ${empty_list}

    ${HEALTHCHECK_OFFLINE} =    Run Process    ${NEOFS_CLI_EXEC} control healthcheck --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG}    shell=True
                            Should Be Equal As Integers    ${HEALTHCHECK_OFFLINE.rc}    0
                            Should Not Be Equal    ${HEALTHCHECK.stdout}    ${HEALTHCHECK_OFFLINE.stdout} 
    
                            Run Process    ${NEOFS_CLI_EXEC} control set-status --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG} --status 'online'    shell=True

                            Sleep    ${MAINNET_BLOCK_TIME}
                            Tick Epoch

    ${SNAPSHOT_ONLINE} =    Run Process    ${NEOFS_CLI_EXEC} control netmap-snapshot --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG}    shell=True
    ${NODE_NUM_ONLINE} =    Get Regexp Matches        ${SNAPSHOT_ONLINE.stdout}    ${NODE_NUM}    
                            Should Be Equal    ${NODE_NUM_ONLINE}[0]    ${NODE_NUM}

    ${HEALTHCHECK_ONLINE} =    Run Process    ${NEOFS_CLI_EXEC} control healthcheck --endpoint ${NODE} --wallet ${STORAGE_WALLET} --config ${WALLET_CONFIG}    shell=True
                            Should Be Equal As Integers    ${HEALTHCHECK_ONLINE.rc}    0
                            Should Be Equal    ${HEALTHCHECK.stdout}    ${HEALTHCHECK_ONLINE.stdout}    

    [Teardown]    Teardown    netmap_control
