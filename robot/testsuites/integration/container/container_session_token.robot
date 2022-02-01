*** Settings ***
Variables    common.py
Variables    wellknown_acl.py

Resource    setup_teardown.robot
Resource    payment_operations.robot

Library     Process
Library     neofs.py
Library     String
Library     OperatingSystem

*** Variables ***
${SIGNED_FILE} =    ${ASSETS_DIR}/signed_token.json

*** Test Cases ***

Session Token for Container
    [Documentation]    Testcase to check container session token
    [Tags]             Container    SessionToken
    [Timeout]          5 min

    [Setup]            Setup

    ${WALLET}    ${OWNER}    ${_} =   Prepare Wallet And Deposit
    ${GEN_WALLET}    ${GEN}    ${_} =    Prepare Wallet And Deposit

    ${UTIL} =    Run Process    ${NEOGO_EXECUTABLE} wallet dump-keys -w ${GEN_WALLET}     shell=True
    ${PUB_PART} =    Get Line    ${UTIL.stdout}    1

    ${SESSION_TOKEN} =    Generate Session Token    ${OWNER}    ${PUB_PART}    wildcard=True

    Sign Session token    ${SESSION_TOKEN}    ${WALLET}    ${SIGNED_FILE}
    
    ${CID} =            Create Container    ${GEN_WALLET}    ${PRIVATE_ACL_F}    ${COMMON_PLACEMENT_RULE}    ${EMPTY}    ${SIGNED_FILE}
                        Wait Until Keyword Succeeds    ${MORPH_BLOCK_TIME}    ${CONTAINER_WAIT_INTERVAL}
                        ...  Container Existing    ${WALLET}    ${CID}
                        Run Keyword And Expect Error    *
                        ...  Container Existing    ${GEN_WALLET}    ${CID}  

########################
# Check container owner 
########################

    ${CONTAINER_INFO} =    Run Process    ${NEOFS_CLI_EXEC} container get --cid ${CID} --wallet ${GEN_WALLET} --config ${WALLET_PASS} --rpc-endpoint ${NEOFS_ENDPOINT}    shell=True
    ${CID_OWNER_ID_LINE} =    Get Line    ${CONTAINER_INFO.stdout}    2
    @{CID_OWNER_ID} =    Split String    ${CID_OWNER_ID_LINE}
    Should Be Equal As Strings    ${OWNER}    ${CID_OWNER_ID}[2]

    [Teardown]        Teardown    container_session_token                   
