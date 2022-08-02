*** Settings ***
Variables   common.py

Library     acl.py
Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    eacl_tables.robot
Resource    common_steps_acl_bearer.robot
Resource    payment_operations.robot
Resource    setup_teardown.robot
Resource    verbs.robot

*** Variables ***
${EACL_ERROR_MSG} =     code = 2048 message = access to object operation denied

*** Test cases ***
BearerToken Operations with Filter Requst Equal
    [Documentation]         Testcase to validate NeoFS operations with BearerToken with Filter Requst Equal.
    [Tags]                  ACL   BearerToken
    [Timeout]               5 min


    Check eACL Deny and Allow All Bearer Filter Requst Equal    Simple
    Check eACL Deny and Allow All Bearer Filter Requst Equal    Complex




*** Keywords ***

Check eACL Deny and Allow All Bearer Filter Requst Equal
    [Arguments]    ${COMPLEXITY}

    ${WALLET}
    ...     ${_}
    ...     ${_} =   Prepare Wallet And Deposit
    ${CID} =        Create Container    ${WALLET}    basic_acl=eacl-public-read-write

    ${OID} =        Run All Verbs Except Delete And Expect Success
                    ...     ${WALLET}   ${CID}      ${COMPLEXITY}

                    Delete Object And Validate Tombstone
                    ...     ${WALLET}   ${CID}      ${OID}

    # Generating empty file to test operations with it after EACL will be set;
    # the size does not matter as we expect to get "operation is not allowed" error
    ${FILE}
    ...     ${_} =      Generate File   0
    ${OID} =            Put object      ${WALLET}    ${FILE}    ${CID}

                        Set eACL        ${WALLET}    ${CID}     ${EACL_DENY_ALL_USER}

                        # The current ACL cache lifetime is 30 sec
                        Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

    ${filters}=         Create Dictionary    headerType=REQUEST        matchType=STRING_EQUAL    key=a    value=256
    ${rule1}=           Create Dictionary    Operation=GET             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule2}=           Create Dictionary    Operation=HEAD            Access=ALLOW    Role=USER    Filters=${filters}
    ${rule3}=           Create Dictionary    Operation=PUT             Access=ALLOW    Role=USER    Filters=${filters}
    ${rule4}=           Create Dictionary    Operation=DELETE          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule5}=           Create Dictionary    Operation=SEARCH          Access=ALLOW    Role=USER    Filters=${filters}
    ${rule6}=           Create Dictionary    Operation=GETRANGE        Access=ALLOW    Role=USER    Filters=${filters}
    ${rule7}=           Create Dictionary    Operation=GETRANGEHASH    Access=ALLOW    Role=USER    Filters=${filters}
    ${eACL_gen}=        Create List    ${rule1}    ${rule2}    ${rule3}    ${rule4}    ${rule5}    ${rule6}    ${rule7}

    ${EACL_TOKEN} =     Form BearerToken File      ${WALLET}    ${CID}   ${eACL_gen}

                        Run All Verbs And Expect Failure
                        ...     ${EACL_ERROR_MSG}   ${WALLET}   ${CID}  ${OID}

    ${OID} =            Run All Verbs Except Delete And Expect Success
                        ...     ${WALLET}   ${CID}      ${COMPLEXITY}
                        ...     ${EACL_TOKEN}       --xhdr a=256

                        Delete Object And Validate Tombstone
                        ...     ${WALLET}   ${CID}      ${OID}
                        ...     ${EACL_TOKEN}       --xhdr a=256
