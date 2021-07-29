*** Settings ***
Variables       ../../../variables/common.py

Library         ../${RESOURCES}/neofs.py
Library         ../${RESOURCES}/payment_neogo.py
Library         Collections

Resource        common_steps_acl_extended.robot
Resource        ../${RESOURCES}/payment_operations.robot
Resource        ../${RESOURCES}/setup_teardown.robot
Resource        ../../../variables/eacl_tables.robot

*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

                            Generate Keys

                            Log    Check extended ACL with simple object
                            Generate files    ${SIMPLE_OBJ_SIZE}
                            Check Filters

                            Log    Check extended ACL with complex object
                            Generate files    ${COMPLEX_OBJ_SIZE}
                            Check Filters

    [Teardown]              Teardown    acl_extended_filters


*** Keywords ***

Check Filters
                            Check eACL MatchType String Equal Object
                            Check eACL MatchType String Not Equal Object
                            Check eACL MatchType String Equal Request Deny
                            Check eACL MatchType String Equal Request Allow

Check eACL MatchType String Equal Request Deny
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}    json_output=True
    &{HEADER_DICT} =        Decode Object System Header Json      ${HEADER}
                            Get object           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl


    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID

                            Set eACL                        ${USER_KEY}    ${CID}    ${EACL_XHEADER_DENY_ALL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl    ${EMPTY}    --xhdr a=2
                            Get object           ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl    ${EMPTY}    --xhdr a=256

                            Run Keyword And Expect Error    *
                            ...  Put object        ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}      --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}      --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}      --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}              --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256         --xhdr a="2"
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=2
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       --xhdr a=2

                            Put object                      ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}        --xhdr a=256
                            Get object                      ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}        --xhdr a=*
                            Search object                   ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}        --xhdr a=
                            Head object                     ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}              --xhdr a=.*
                            Get Range                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256           --xhdr a="2 2"
                            Get Range Hash                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=256
                            Delete object                   ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       --xhdr a=22


Check eACL MatchType String Equal Request Allow
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object             ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}    json_output=True
    &{HEADER_DICT} =        Decode Object System Header Json      ${HEADER}
                            Get object           ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl


    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID

                            Set eACL                        ${USER_KEY}    ${CID}    ${EACL_XHEADER_ALLOW_ALL}

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Get eACL                        ${USER_KEY}    ${CID}

                            Run Keyword And Expect Error    *
                            ...  Get object                 ${OTHER_KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}    local_file_eacl    ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...  Put object                 ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...  Get object                 ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...   Search object             ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...  Head object                ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256
                            Run Keyword And Expect Error    *
                            ...  Get Range Hash             ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256
                            Run Keyword And Expect Error    *
                            ...  Delete object              ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}

                            Put object                      ${OTHER_KEY}    ${FILE_S}     ${CID}           ${EMPTY}       ${FILE_OTH_HEADER}    ${EMPTY}        --xhdr a=2
                            Get object                      ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       local_file_eacl       ${EMPTY}        --xhdr a=2
                            Search object                   ${OTHER_KEY}    ${CID}        ${EMPTY}         ${EMPTY}       ${FILE_USR_HEADER}    ${EMPTY}        --xhdr a=2
                            Head object                     ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       ${EMPTY}              --xhdr a=2
                            Get Range                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}    s_get_range    ${EMPTY}              0:256           --xhdr a=2
                            Get Range Hash                  ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       0:256                 --xhdr a=2
                            Delete object                   ${OTHER_KEY}    ${CID}        ${S_OID_USER}    ${EMPTY}       --xhdr a=2


Check eACL MatchType String Equal Object
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object                      ${USER_KEY}     ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}

    ${HEADER} =             Head object                     ${USER_KEY}     ${CID}       ${S_OID_USER}    ${EMPTY}    json_output=True
    &{HEADER_DICT} =        Decode Object System Header Json      ${HEADER}
                            Get object                      ${OTHER_KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object ID
    ${ID_value} =	        Get From Dictionary	            ${HEADER_DICT}    ID

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=$Object:objectID    value=${ID_value}
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY               Role=OTHERS             Filters=${filters}
    ${eACL_gen} =           Create List          ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    ${ASSETS_DIR}/eacl_custom    ${eACL_gen}

                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object                 ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}        local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header
    ${S_OID_USER_OTH} =     Put object             ${USER_KEY}     ${FILE_S}    ${CID}               ${EMPTY}        ${FILE_OTH_HEADER}

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_EQUAL    key=key1    value=1
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY               Role=OTHERS             Filters=${filters}
    ${eACL_gen} =           Create List    ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    ${ASSETS_DIR}/eacl_custom    ${eACL_gen}


                            Set eACL                        ${USER_KEY}     ${CID}       ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}       ${S_OID_USER}        ${EMPTY}        local_file_eacl
                            Get object           ${OTHER_KEY}    ${CID}       ${S_OID_USER_OTH}    ${EMPTY}        local_file_eacl



Check eACL MatchType String Not Equal Object
    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object             ${USER_KEY}     ${FILE_S}      ${CID}    ${EMPTY}    ${FILE_USR_HEADER}
    ${S_OID_OTHER} =        Put object             ${OTHER_KEY}    ${FILE_S_2}    ${CID}    ${EMPTY}    ${FILE_OTH_HEADER}

    ${HEADER} =             Head object                     ${USER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}    json_output=True
                            Head object                     ${USER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    json_output=True

    &{HEADER_DICT} =        Decode Object System Header Json      ${HEADER}

                            Get object           ${OTHER_KEY}    ${CID}    ${S_OID_USER}     ${EMPTY}    local_file_eacl
                            Get object           ${OTHER_KEY}    ${CID}    ${S_OID_OTHER}    ${EMPTY}    local_file_eacl

                            Log	                            Set eACL for Deny GET operation with StringNotEqual Object ID
    ${ID_value} =	    Get From Dictionary	            ${HEADER_DICT}    ID

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_NOT_EQUAL    key=$Object:objectID    value=${ID_value}
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY                   Role=OTHERS             Filters=${filters}
    ${eACL_gen} =           Create List    ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    ${ASSETS_DIR}/eacl_custom    ${eACL_gen}

                            Set eACL                        ${USER_KEY}       ${CID}    ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}      ${CID}    ${S_OID_OTHER}    ${EMPTY}            local_file_eacl
                            Get object           ${OTHER_KEY}      ${CID}    ${S_OID_USER}     ${EMPTY}            local_file_eacl


                            Log	                            Set eACL for Deny GET operation with StringEqual Object Extended User Header
    ${S_OID_USER_OTH} =     Put object             ${USER_KEY}    ${FILE_S}    ${CID}               ${EMPTY}            ${FILE_OTH_HEADER}

    ${filters} =            Create Dictionary    headerType=OBJECT    matchType=STRING_NOT_EQUAL    key=key1       value=1
    ${rule1} =              Create Dictionary    Operation=GET        Access=DENY                   Role=OTHERS    Filters=${filters}
    ${eACL_gen} =           Create List    ${rule1}
    ${EACL_CUSTOM} =        Form eACL json common file    ${ASSETS_DIR}/eacl_custom    ${eACL_gen}

                            Set eACL                        ${USER_KEY}    ${CID}       ${EACL_CUSTOM}
                            Run Keyword And Expect Error    *
                            ...  Get object      ${OTHER_KEY}    ${CID}      ${S_OID_USER_OTH}    ${EMPTY}            local_file_eacl
                            Get object           ${OTHER_KEY}    ${CID}      ${S_OID_USER}        ${EMPTY}            local_file_eacl
