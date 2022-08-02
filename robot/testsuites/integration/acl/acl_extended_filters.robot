*** Settings ***
Variables       common.py

Library         acl.py
Library         container.py
Library         neofs_verbs.py
Library         Collections

Resource        common_steps_acl_extended.robot
Resource        payment_operations.robot
Resource        setup_teardown.robot
Resource        eacl_tables.robot

*** Variables ***
${PATH} =   testfile
&{USER_HEADER} =        key1=1      key2=abc
&{ANOTHER_HEADER} =     key1=oth    key2=oth
${ID_FILTER} =          $Object:objectID
${CUSTOM_FILTER} =      $Object:key1

*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL
    [Timeout]               2 min


    Check Filters    Simple
    Check Filters    Complex



*** Keywords ***

Check Filters
    [Arguments]    ${OBJ_COMPLEXITY}

    ${SIZE} =     Set Variable IF
    ...  """${OBJ_COMPLEXITY}""" == """Simple"""    ${SIMPLE_OBJ_SIZE}    ${COMPLEX_OBJ_SIZE}

    ${WALLET}        ${_}    ${_} =    Prepare Wallet And Deposit
    ${WALLET_OTH}    ${_}    ${_} =    Prepare Wallet And Deposit
    ${FILE_S}        ${_} =            Generate File    ${SIZE}

    Check eACL MatchType String Equal Object           ${WALLET}    ${WALLET_OTH}    ${FILE_S}
    Check eACL MatchType String Not Equal Object       ${WALLET}    ${WALLET_OTH}    ${FILE_S}
    Check eACL MatchType String Equal Request Deny     ${WALLET}    ${WALLET_OTH}    ${FILE_S}
    Check eACL MatchType String Equal Request Allow    ${WALLET}    ${WALLET_OTH}    ${FILE_S}


Check eACL MatchType String Equal Request Deny
    [Arguments]    ${USER_WALLET}    ${OTHER_WALLET}    ${FILE_S}
    ${CID} =                Create Container       ${USER_WALLET}    basic_acl=eacl-public-read-write
    ${S_OID_USER} =         Put object             ${USER_WALLET}    ${FILE_S}    ${CID}      user_headers=${USER_HEADER}
                            Get object             ${USER_WALLET}    ${CID}       ${S_OID_USER}    ${EMPTY}    ${PATH}

                            Set eACL               ${USER_WALLET}    ${CID}    ${EACL_XHEADER_DENY_ALL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${PATH}    ${EMPTY}    --xhdr a=2
                            Get object           ${OTHER_WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${PATH}    ${EMPTY}    --xhdr a=256

                            Run Keyword And Expect Error    *
                            ...  Put object        ${OTHER_WALLET}    ${FILE_S}     ${CID}      user_headers=${ANOTHER_HEADER}    options=--xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${PATH}       ${EMPTY}      --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_WALLET}    ${CID}        ${EMPTY}         ${EMPTY}       ${USER_HEADER}    ${EMPTY}      --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256         options=--xhdr a="2"
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       --xhdr a=2

                            Put object                      ${OTHER_WALLET}    ${FILE_S}     ${CID}           user_headers=${ANOTHER_HEADER}    options=--xhdr a=256
                            Get object                      ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${PATH}       ${EMPTY}        --xhdr a=*
                            Search object                   ${OTHER_WALLET}    ${CID}        ${EMPTY}         ${EMPTY}       ${USER_HEADER}    ${EMPTY}        --xhdr a=
                            Head object                     ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=.*
                            Get Range                       ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256           options=--xhdr a="2 2"
                            Get Range Hash                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=256
                            Delete object                   ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=22


Check eACL MatchType String Equal Request Allow
    [Arguments]    ${USER_WALLET}    ${OTHER_WALLET}    ${FILE_S}

    ${CID} =                Create Container    ${USER_WALLET}      basic_acl=eacl-public-read-write
    ${S_OID_USER} =         Put Object          ${USER_WALLET}      ${FILE_S}   ${CID}
                            Get Object          ${OTHER_WALLET}     ${CID}      ${S_OID_USER}    ${EMPTY}    ${PATH}

                            Set eACL    ${USER_WALLET}    ${CID}    ${EACL_XHEADER_ALLOW_ALL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Get eACL                        ${USER_WALLET}    ${CID}

                            Run Keyword And Expect Error    *
                            ...  Get object                 ${OTHER_WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${PATH}    ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...  Put object                 ${OTHER_WALLET}    ${FILE_S}     ${CID}
                            Run Keyword And Expect Error    *
                            ...  Get object                 ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${PATH}       ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_WALLET}    ${CID}        ${EMPTY}         ${EMPTY}       ${USER_HEADER}    ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_WALLET}    ${CID}        ${S_OID_USER}
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_WALLET}    ${CID}        ${S_OID_USER}

                            Put object                      ${OTHER_WALLET}    ${FILE_S}     ${CID}           options=--xhdr a=2
                            Get object                      ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${PATH}       ${EMPTY}        --xhdr a=2
                            Search object                   ${OTHER_WALLET}    ${CID}        ${EMPTY}         ${EMPTY}       ${USER_HEADER}    ${EMPTY}        --xhdr a=2
                            Head object                     ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2
                            Get Range                       ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    0:256           options=--xhdr a=2
                            Get Range Hash                  ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=2
                            Delete object                   ${OTHER_WALLET}    ${CID}        ${S_OID_USER}    options=--xhdr a=2


Check eACL MatchType String Equal Object
    [Arguments]    ${USER_WALLET}    ${OTHER_WALLET}    ${FILE_S}

    ${CID} =                Create Container       ${USER_WALLET}   basic_acl=eacl-public-read-write
    ${S_OID_USER} =         Put Object    ${USER_WALLET}     ${FILE_S}    ${CID}    user_headers=${USER_HEADER}
                            Get Object    ${OTHER_WALLET}    ${CID}       ${S_OID_USER}    ${EMPTY}    ${PATH}

                            Log    Set eACL for Deny GET operation with StringEqual Object ID

    &{HEADER_DICT} =        Head Object    ${USER_WALLET}     ${CID}       ${S_OID_USER}
    ${ID_value} =           Get From dictionary    ${HEADER_DICT}    ${EACL_OBJ_FILTERS}[${ID_FILTER}]

    ${filters} =            Set Variable    obj:${ID_FILTER}=${ID_value}
    ${rule1} =              Set Variable    deny get ${filters} others
    ${eACL_gen} =           Create List     ${rule1}
    ${EACL_CUSTOM} =        Create eACL     ${CID}    ${eACL_gen}
                            Set eACL        ${USER_WALLET}       ${CID}    ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object    ${OTHER_WALLET}      ${CID}    ${S_OID_USER}     ${EMPTY}    ${PATH}

                            Log	                 Set eACL for Deny GET operation with StringEqual Object Extended User Header

    ${S_OID_USER_OTH} =     Put object           ${USER_WALLET}     ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}

    ${filters} =            Set Variable    obj:${CUSTOM_FILTER}=1
    ${rule1} =              Set Variable    deny get ${filters} others
    ${eACL_gen} =           Create List     ${rule1}
    ${EACL_CUSTOM} =        Create eACL     ${CID}    ${eACL_gen}

                            Set eACL             ${USER_WALLET}     ${CID}       ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_WALLET}    ${CID}    ${S_OID_USER}    ${EMPTY}    ${PATH}
                            Get object           ${OTHER_WALLET}    ${CID}    ${S_OID_USER_OTH}    ${EMPTY}    ${PATH}


Check eACL MatchType String Not Equal Object
    [Arguments]    ${USER_WALLET}    ${OTHER_WALLET}    ${FILE_S}

    ${CID} =                Create Container   ${USER_WALLET}       basic_acl=eacl-public-read-write

    ${S_OID_USER} =         Put object         ${USER_WALLET}     ${FILE_S}      ${CID}    user_headers=${USER_HEADER}
    ${S_OID_OTHER} =        Put object         ${OTHER_WALLET}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
                            Get object          ${OTHER_WALLET}    ${CID}    ${S_OID_USER}     ${EMPTY}    ${PATH}
                            Get object          ${OTHER_WALLET}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    ${PATH}

                            Log	                    Set eACL for Deny GET operation with StringNotEqual Object ID

    &{HEADER_DICT} =        Head object        ${USER_WALLET}    ${CID}    ${S_OID_USER}
    ${ID_value} =           Get From Dictionary	    ${HEADER_DICT}    ${EACL_OBJ_FILTERS}[${ID_FILTER}]

    ${filters} =            Set Variable    obj:${ID_FILTER}!=${ID_value}
    ${rule1} =              Set Variable    deny get ${filters} others
    ${eACL_gen} =           Create List     ${rule1}
    ${EACL_CUSTOM} =        Create eACL     ${CID}    ${eACL_gen}

                            Set eACL        ${USER_WALLET}       ${CID}    ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_WALLET}      ${CID}    ${S_OID_OTHER}    ${EMPTY}            ${PATH}
                            Get object           ${OTHER_WALLET}      ${CID}    ${S_OID_USER}     ${EMPTY}            ${PATH}

                            Log	               Set eACL for Deny GET operation with StringEqual Object Extended User Header

    ${S_OID_USER_OTH} =     Put object          ${USER_WALLET}    ${FILE_S}    ${CID}    user_headers=${ANOTHER_HEADER}
    ${filters} =            Set Variable        obj:${CUSTOM_FILTER}!=1
    ${rule1} =              Set Variable        deny get ${filters} others
    ${eACL_gen} =           Create List         ${rule1}
    ${EACL_CUSTOM} =        Create eACL         ${CID}    ${eACL_gen}

                            Set eACL             ${USER_WALLET}    ${CID}       ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_WALLET}    ${CID}      ${S_OID_USER_OTH}    ${EMPTY}            ${PATH}
                            Get object           ${OTHER_WALLET}    ${CID}      ${S_OID_USER}        ${EMPTY}            ${PATH}
