*** Settings ***
Variables   common.py

Library     storage_group.py
Library     Collections

Resource    complex_object_operations.robot

*** Variables ***
${PERMISSION_ERROR} =   status: code = 2048 message = access to object operation denied
${DEPOSIT} =            ${30}

*** Keywords ***

Run Storage Group Operations And Expect Success
    [Arguments]         ${WALLET}   ${CID}  ${OBJECTS}      ${OBJ_COMPLEXITY}
    [Documentation]     This keyword verifies if Object's owner is granted to
            ...         Put, List, Get and Delete a Storage Group which contains
            ...         the Object.

    ${SG} =     Put Storagegroup            ${WALLET}   ${CID}  ${OBJECTS}
                Verify List Storage Group   ${WALLET}   ${CID}  ${SG}
                Verify Get Storage Group    ${WALLET}   ${CID}  ${SG}   ${OBJECTS}  ${OBJ_COMPLEXITY}
                Delete Storagegroup         ${WALLET}   ${CID}  ${SG}


Run Storage Group Operations And Expect Failure
    [Arguments]         ${WALLET}   ${CID}  ${OBJECTS}  ${SG}
    [Documentation]     This keyword verifies if Object's owner isn't granted to
            ...         Put, List, Get and Delete a Storage Group which contains
            ...         the Object.

    ${ERR} =        Run Keyword And Expect Error        *
                    ...  Put Storagegroup   ${WALLET}    ${CID}   ${OBJECTS}
                    Should Contain          ${ERR}  ${PERMISSION_ERROR}
    ${ERR} =        Run Keyword And Expect Error        *
                    ...  List Storagegroup    ${WALLET}    ${CID}
                    Should Contain          ${ERR}  ${PERMISSION_ERROR}
    ${ERR} =        Run Keyword And Expect Error        *
                    ...  Get Storagegroup    ${WALLET}    ${CID}    ${SG}
                    Should Contain          ${ERR}  ${PERMISSION_ERROR}
    ${ERR} =        Run Keyword And Expect Error        *
                    ...  Delete Storagegroup    ${WALLET}    ${CID}    ${SG}
                    Should Contain          ${ERR}  ${PERMISSION_ERROR}


Run Storage Group Operations With Bearer Token
    [Arguments]         ${WALLET}   ${CID}  ${OBJECTS}      ${BEARER}     ${OBJ_COMPLEXITY}

    ${SG} =    Put Storagegroup             ${WALLET}   ${CID}  ${OBJECTS}  bearer_token=${BEARER}
               Verify List Storage Group    ${WALLET}   ${CID}  ${SG}   ${BEARER}
               Verify Get Storage Group     ${WALLET}   ${CID}  ${SG}   ${OBJECTS}  ${OBJ_COMPLEXITY}   ${BEARER}
               Delete Storagegroup          ${WALLET}   ${CID}  ${SG}   bearer_token=${BEARER}


Run Storage Group Operations On Other's Behalf In RO Container
    [Arguments]         ${OWNER_WALLET}     ${CID}  ${OBJECTS}    ${OBJ_COMPLEXITY}
    [Documentation]     ${OWNER_WALLET}:    ${OBJECTS}' owner wallet
            ...         ${CID}:             ID of read-only container
            ...         ${OBJECTS}:         list of Object IDs to include into the Storage Group
            ...         ${OBJ_COMPLEXITY}:  [Complex|Simple]

    ${OTHER_WALLET}    ${_}    ${_} =   Prepare Wallet And Deposit

    ${SG} =     Put Storagegroup        ${OWNER_WALLET}    ${CID}   ${OBJECTS}
    ${ERR} =    Run Keyword And Expect Error        *
                ...     Put Storagegroup        ${OTHER_WALLET}    ${CID}   ${OBJECTS}
                Should Contain      ${ERR}  ${PERMISSION_ERROR}

                Verify List Storage Group   ${OTHER_WALLET}   ${CID}  ${SG}
                Verify Get Storage Group    ${OTHER_WALLET}   ${CID}  ${SG}   ${OBJECTS}  ${OBJ_COMPLEXITY}

    ${ERR} =    Run Keyword And Expect Error    *
                    ...     Delete Storagegroup     ${OTHER_WALLET}    ${CID}    ${SG}
                Should Contain      ${ERR}          ${PERMISSION_ERROR}


Run Storage Group Operations On System's Behalf In RO Container
    [Arguments]         ${CID}  ${OBJECTS}    ${OBJ_COMPLEXITY}
    [Documentation]     ${CID}:             ID of read-only container
            ...         ${OBJECTS}:         list of Object IDs to include into the Storage Group
            ...         ${OBJ_COMPLEXITY}:  [Complex|Simple]
            ...
            ...         In this keyword we create Storage Group on Inner Ring's key behalf
            ...         and include an Object created on behalf of some user. We expect
            ...         that System key is granted to make all operations except DELETE.

                Transfer Mainnet Gas        ${IR_WALLET_PATH}    ${DEPOSIT + 1}  wallet_password=${IR_WALLET_PASS}
                NeoFS Deposit               ${IR_WALLET_PATH}    ${DEPOSIT}      wallet_password=${IR_WALLET_PASS}

    ${SG} =     Put Storagegroup            ${IR_WALLET_PATH}    ${CID}  ${OBJECTS}     wallet_config=${IR_WALLET_CONFIG}
                Verify List Storage Group   ${IR_WALLET_PATH}    ${CID}  ${SG}          WALLET_CFG=${IR_WALLET_CONFIG}
                Verify Get Storage Group    ${IR_WALLET_PATH}    ${CID}  ${SG}   ${OBJECTS}  ${OBJ_COMPLEXITY}      WALLET_CFG=${IR_WALLET_CONFIG}
    ${ERR} =    Run Keyword And Expect Error    *
                ...     Delete Storagegroup    ${IR_WALLET_PATH}    ${CID}    ${SG}     wallet_config=${IR_WALLET_CONFIG}
                Should Contain      ${ERR}      ${PERMISSION_ERROR}


Verify List Storage Group
    [Arguments]         ${WALLET}   ${CID}  ${SG}   ${BEARER}=${EMPTY}  ${WALLET_CFG}=${WALLET_CONFIG}

    @{STORAGE_GROUPS} =     List Storagegroup           ${WALLET}           ${CID}     bearer_token=${BEARER}   wallet_config=${WALLET_CFG}
                            List Should Contain Value   ${STORAGE_GROUPS}   ${SG}
                            ...     msg="Storage Group hasn't been persisted"


Verify Get Storage Group
    [Arguments]         ${WALLET}   ${CID}  ${SG}   ${OBJECTS}   ${OBJ_COMPLEXITY}     ${BEARER}=${EMPTY}   ${WALLET_CFG}=${WALLET_CONFIG}

    @{PART_OIDS} =      Create List
    IF    """${OBJ_COMPLEXITY}""" == """Complex"""
        FOR     ${OBJ}      IN      @{OBJECTS}
            ${OIDS} =       Get Object Parts By Link Object
                            ...     ${WALLET}    ${CID}   ${OBJ}    BEARER=${BEARER}   WALLET_CFG=${WALLET_CFG}
            @{PART_OIDS} =  Combine Lists       ${PART_OIDS}    ${OIDS}
        END
    END

    ${OBJECTS_NUMBER} =     Get Length      ${OBJECTS}

    &{SG_DATA} =        Get Storagegroup        ${WALLET}    ${CID}    ${SG}    bearer_token=${BEARER}   wallet_config=${WALLET_CFG}

    IF      """${OBJ_COMPLEXITY}""" == """Simple"""
        ${EXPECTED_SIZE} =      Evaluate        ${SIMPLE_OBJ_SIZE} * ${OBJECTS_NUMBER}
        Should Be Equal As Numbers              ${SG_DATA}[Group size]    ${EXPECTED_SIZE}
        Lists Should Be Equal   ${SG_DATA}[Members]       ${OBJECTS}
    ELSE
        ${EXPECTED_SIZE} =      Evaluate        ${COMPLEX_OBJ_SIZE} * ${OBJECTS_NUMBER}
        Should Be Equal As Numbers              ${SG_DATA}[Group size]    ${EXPECTED_SIZE}
        Lists Should Be Equal   ${SG_DATA}[Members]       ${PART_OIDS}
    END
