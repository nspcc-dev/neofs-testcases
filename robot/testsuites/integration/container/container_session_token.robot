*** Settings ***
Variables    common.py

Resource    payment_operations.robot

Library     container.py
Library     session_token.py

*** Test Cases ***

Session Token for Container
    [Documentation]    Testcase to check container session token
    [Tags]             Container    SessionToken
    [Timeout]          5 min


    ${OWNER_WALLET}    ${OWNER}    ${_} =   Prepare Wallet And Deposit
    ${SESSION_WALLET}    ${_}    ${_} =    Prepare Wallet And Deposit

    ${SESSION_TOKEN} =      Generate Session Token      ${OWNER}    ${SESSION_WALLET}
    ${SIGNED_FILE} =        Sign Session token          ${SESSION_TOKEN}   ${OWNER_WALLET}

    ${CID} =                Create Container    ${OWNER_WALLET}
                            ...     session_token=${SIGNED_FILE}    session_wallet=${SESSION_WALLET}

########################
# Check container owner
########################

    &{ATTRS} =      Get Container                 ${SESSION_WALLET}  ${CID}
                    Should Be Equal As Strings    ${OWNER}    ${ATTRS}[ownerID]

