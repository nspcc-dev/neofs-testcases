*** Settings ***
Variables   common.py

Library    Collections
Library    Process
Library    String
Library    epoch.py

Resource    payment_operations.robot

*** Variables ***
${SN_01_ADDR} =     s01.neofs.devenv:8080
${SN_02_ADDR} =     s02.neofs.devenv:8080
${DEPOSIT} =        ${30}

*** Test cases ***
NetworkInfo RPC Method
    [Documentation]           Testcase to check NetworkInfo RPC method.
    [Tags]                    RPC  NetworkInfo
    [Timeout]                 10 min


    ######################################################################
    # Checking if the command returns equal results for two storage nodes
    ######################################################################
    ${RESULT1_S01} =        Run Process    ${NEOFS_CLI_EXEC} netmap netinfo -r ${SN_01_ADDR} --wallet ${STORAGE_WALLET_PATH} --config ${WALLET_CONFIG}
                            ...             shell=True
                            Should Be Equal As Integers    ${RESULT1_S01.rc} 	0
    ${RESULT1_S02} =        Run Process    ${NEOFS_CLI_EXEC} netmap netinfo -r ${SN_02_ADDR} --wallet ${STORAGE_WALLET_PATH} --config ${WALLET_CONFIG}
                            ...             shell=True
                            Should Be Equal As Integers    ${RESULT1_S02.rc} 	0

    #############################################
    # Checking if morph magic number is relevant
    #############################################

    ${NETWORK_MAGIC_S01} =  Parse Magic    ${RESULT1_S01.stdout}
                            Should Be Equal    ${NETWORK_MAGIC_S01}    ${MORPH_MAGIC}

    ${NETWORK_MAGIC_S02} =  Parse Magic    ${RESULT1_S02.stdout}
                            Should Be Equal    ${NETWORK_MAGIC_S02}    ${MORPH_MAGIC}

    #######################################################################
    # Checking if epoch numbers requested from two storage nodes are equal
    #######################################################################

    ${EPOCH1_S01} =         Parse Epoch    ${RESULT1_S01.stdout}
    ${EPOCH1_S02} =         Parse Epoch    ${RESULT1_S02.stdout}
                            Should Be Equal As Integers    ${EPOCH1_S01}    ${EPOCH1_S02}

    ########################################
    # Ticking epoch and getting new netinfo
    ########################################

                            Tick Epoch

    ${RESULT2_S01} =        Run Process    ${NEOFS_CLI_EXEC} netmap netinfo -r ${SN_01_ADDR} --wallet ${STORAGE_WALLET_PATH} --config ${WALLET_CONFIG}
                            ...             shell=True
                            Should Be Equal As Integers    ${RESULT2_S01.rc} 	0
    ${RESULT2_S02} =        Run Process    ${NEOFS_CLI_EXEC} netmap netinfo -r ${SN_02_ADDR} --wallet ${STORAGE_WALLET_PATH} --config ${WALLET_CONFIG}
                            ...             shell=True
                            Should Be Equal As Integers    ${RESULT2_S02.rc} 	0

                            Should Be Equal As Strings    ${RESULT2_S01.stdout}    ${RESULT2_S02.stdout}

    ${EPOCH2_S01} =         Parse Epoch    ${RESULT2_S01.stdout}

    #################################################################
    # Checking if the second epoch value is more than the first by 1
    #################################################################

    ${NEW_EPOCH} =          Evaluate    ${EPOCH1_S01}+${1}
                            Should Be Equal    ${EPOCH2_S01}    ${NEW_EPOCH}



*** Keywords ***

Parse Magic
    [Arguments]    ${RESULT_STDOUT}
    @{MAGIC} =               Split String    ${RESULT_STDOUT}   ${\n}
    ${NETWORK_MAGIC} =       Get From List    ${MAGIC}    ${1}
    @{MAGIC_INFO} =          Split String    ${NETWORK_MAGIC}   ${SPACE}
    ${MAGIC_VALUE} =         Get From List    ${MAGIC_INFO}    ${4}
    [Return]    ${MAGIC_VALUE}

Parse Epoch
    [Arguments]    ${RESULT_STDOUT}
    @{EPOCH} =               Split String    ${RESULT_STDOUT}   ${\n}
    ${NETWORK_EPOCH} =       Get From List    ${EPOCH}    ${0}
    @{EPOCH_INFO} =          Split String    ${NETWORK_EPOCH}    ${SPACE}
    ${EPOCH_VALUE} =         Get From List    ${EPOCH_INFO}    ${1}
    ${EPOCH_VALUE_INT} =     Convert To Integer    ${EPOCH_VALUE}
    [Return]    ${EPOCH_VALUE_INT}
