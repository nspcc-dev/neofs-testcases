*** Settings ***
Variables   common.py

Library    Collections
Library    Process
Library    String
Library    utility_keywords.py

Resource    payment_operations.robot

*** Variables ***
${DEPOSIT_AMOUNT} =     ${10}

*** Test cases ***
CLI Accounting Balance Test
    [Documentation]           neofs-cli accounting balance test
    [Timeout]                 10 min


    ${WALLET}   ${ADDR}     ${_} =   Prepare Wallet And Deposit   ${DEPOSIT_AMOUNT}

    # Getting balance with wallet and address
    ${OUTPUT} =     Run Process    ${NEOFS_CLI_EXEC} accounting balance -r ${NEOFS_ENDPOINT} --address ${ADDR} --wallet ${WALLET} --config ${WALLET_CONFIG}
                    ...    shell=True
    Should Be Equal As Numbers   ${OUTPUT.stdout}   ${DEPOSIT_AMOUNT}

    # Getting balance with wallet only
    ${OUTPUT} =     Run Process    ${NEOFS_CLI_EXEC} accounting balance -r ${NEOFS_ENDPOINT} --wallet ${WALLET} --config ${WALLET_CONFIG}
                    ...    shell=True
    Should Be Equal As Numbers   ${OUTPUT.stdout}   ${DEPOSIT_AMOUNT}

    # Getting balance with wallet and wrong address
    ${_}   ${ANOTHER_ADDR}     ${_} =   Generate Wallet
    ${OUTPUT} =     Run Process    ${NEOFS_CLI_EXEC} accounting balance -r ${NEOFS_ENDPOINT} --address ${ANOTHER_ADDR} --wallet ${WALLET} --config ${WALLET_CONFIG}
                    ...    shell=True
    Should Contain                 ${OUTPUT.stderr}    --address option must be specified and valid
    Should Be Equal As Numbers     ${OUTPUT.rc}        1

    # Getting balance with control API
    ${CONFIG_PATH} =    Write API Config    ${NEOFS_ENDPOINT}   ${WALLET}
    ${OUTPUT} =         Run Process     ${NEOFS_CLI_EXEC} accounting balance --config ${CONFIG_PATH}
                        ...    shell=True
    Should Be Equal As Numbers          ${OUTPUT.stdout}   ${DEPOSIT_AMOUNT}


*** Keywords ***

Write API Config
    [Documentation]     Write YAML config for requesting NeoFS API via CLI
    [Arguments]         ${ENDPOINT}     ${WALLET}

    Set Local Variable  ${PATH}     ${ASSETS_DIR}/config.yaml
    Create File         ${PATH}     rpc-endpoint: ${ENDPOINT}\nwallet: ${WALLET}\npassword: ''

    [Return]            ${PATH}
