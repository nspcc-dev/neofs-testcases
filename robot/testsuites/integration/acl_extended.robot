*** Settings ***
Variables   ../../variables/common.py

Library     Collections
Library     ${RESOURCES}/environment.py
Library     ${RESOURCES}/neo.py
Library     ${RESOURCES}/neofs.py


*** Variables ***
&{FILE_USR_HEADER} =        key1=1    key2='abc1'
&{FILE_USR_HEADER_DEL} =    key1=del  key2=del
&{FILE_OTH_HEADER} =        key1=oth  key2=oth

*** Test cases ***
Extended ACL Operations
    [Documentation]     Testcase to validate NeoFS operations with extended ACL.
    [Tags]              ACL  eACL  NeoFS  NeoCLI
    [Timeout]           20 min

    Generate Keys
    Generate file
    Prepare eACL Role rules
    Check Actions
    Check Filters
    
    
 
*** Keywords ***

Check Actions
    Check eACL Deny and Allow All Other
    Check eACL Deny and Allow All User
    Check eACL Deny and Allow All System
    Check eACL Deny All Other and Allow All Pubkey

    
Check Filters
    Check eACL MatchType String Equal
    Check eACL MatchType String Not Equal


Check eACL MatchType String Equal
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}      ${FILE_S}     ${CID}            ${EMPTY}            &{FILE_USR_HEADER} 

    ${HEADER} =             Head object                         ${USER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}     ${True}
    &{SYS_HEADER_PARSED} =  Parse Object System Header          ${HEADER} 
                            
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Log	                                Set eACL for Deny GET operation with StringEqual Object ID
    ${ID_value} =	        Get From Dictionary	                ${SYS_HEADER_PARSED}	ID   
    ${ID_value_hex} =       Convert Str to Hex Str with Len     ${ID_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         000100000002000000010001000000020000000100024944  ${ID_value_hex}  0001000000030000    
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object CID
    ${CID_value} =	        Get From Dictionary	                ${SYS_HEADER_PARSED}	CID   
    ${CID_value_hex} =      Convert Str to Hex Str with Len     ${CID_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         00010000000200000001000100000002000000010003434944  ${CID_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object OwnerID
    ${OwnerID_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	OwnerID   
    ${OwnerID_value_hex} =  Convert Str to Hex Str with Len     ${OwnerID_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         000100000002000000010001000000020000000100084f574e45525f4944  ${OwnerID_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object Version
    ${Version_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	Version   
    ${Version_value_hex} =  Convert Str to Hex Str with Len     ${Version_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000001000756455253494f4e   ${Version_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object PayloadLength
    ${Payload_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	PayloadLength   
    ${Payload_value_hex} =  Convert Str to Hex Str with Len     ${Payload_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000001000e5041594c4f41445f4c454e475448   ${Payload_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Log	                                Set eACL for Deny GET operation with StringEqual Object CreatedAtUnixTime
    ${AtUnixTime_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	CreatedAtUnixTime   
    ${AtUnixTime_value_hex} =   Convert Str to Hex Str with Len     ${AtUnixTime_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000001000c435245415445445f554e4958  ${AtUnixTime_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object CreatedAtEpoch
    ${AtEpoch_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	CreatedAtEpoch   
    ${AtEpoch_value_hex} =  Convert Str to Hex Str with Len     ${AtEpoch_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000001000d435245415445445f45504f4348  ${AtEpoch_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object to NeoFS                 ${USER_KEY}      ${FILE_S}     ${CID}            ${EMPTY}            &{FILE_OTH_HEADER} 
                            Set eACL                            ${USER_KEY}      ${CID}        000100000002000000010001000000030000000100046b65793200062761626331270001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER_OTH}            ${EMPTY}        local_file_eacl


Check eACL MatchType String Not Equal
    ${CID} =                Create Container Public
    ${FILE_S_2} =           Generate file of bytes              2048
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}      ${FILE_S}     ${CID}            ${EMPTY}            &{FILE_USR_HEADER} 
    # Sleep for 1 epoch
                            Sleep                               ${NEOFS_EPOCH_TIMEOUT}
    ${S_OID_OTHER} =        Put object to NeoFS                 ${OTHER_KEY}     ${FILE_S_2}   ${CID}            ${EMPTY}            &{FILE_OTH_HEADER} 
    ${HEADER} =             Head object                         ${USER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}     ${True}
                            Head object                         ${USER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}    ${True}
    &{SYS_HEADER_PARSED} =  Parse Object System Header          ${HEADER} 
                            
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
    
                            Log	                                Set eACL for Deny GET operation with StringNotEqual Object ID
    ${ID_value} =	        Get From Dictionary	                ${SYS_HEADER_PARSED}	ID   
    ${ID_value_hex} =       Convert Str to Hex Str with Len     ${ID_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         000100000002000000010001000000020000000200024944  ${ID_value_hex}  0001000000030000  
                                                                                      
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}             ${EMPTY}            local_file_eacl



                            Log	                                Set eACL for Deny GET operation with StringEqual Object CID
    ${CID_value} =	        Get From Dictionary	                ${SYS_HEADER_PARSED}	CID   
    ${CID_value_hex} =      Convert Str to Hex Str with Len     ${CID_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         00010000000200000001000100000002000000020003434944  ${CID_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object OwnerID
    ${OwnerID_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	OwnerID   
    ${OwnerID_value_hex} =  Convert Str to Hex Str with Len     ${OwnerID_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         000100000002000000010001000000020000000200084f574e45525f4944  ${OwnerID_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}             ${EMPTY}           local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object Version
    ${Version_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	Version   
    ${Version_value_hex} =  Convert Str to Hex Str with Len     ${Version_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000002000756455253494f4e   ${Version_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}             ${EMPTY}           local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object PayloadLength
    ${Payload_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	PayloadLength   
    ${Payload_value_hex} =  Convert Str to Hex Str with Len     ${Payload_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000002000e5041594c4f41445f4c454e475448   ${Payload_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Log	                                Set eACL for Deny GET operation with StringEqual Object CreatedAtUnixTime
    ${AtUnixTime_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	CreatedAtUnixTime   
    ${AtUnixTime_value_hex} =   Convert Str to Hex Str with Len     ${AtUnixTime_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000002000c435245415445445f554e4958  ${AtUnixTime_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}             ${EMPTY}           local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object CreatedAtEpoch
    ${AtEpoch_value} =	    Get From Dictionary	                ${SYS_HEADER_PARSED}	CreatedAtEpoch   
    ${AtEpoch_value_hex} =  Convert Str to Hex Str with Len     ${AtEpoch_value}     
                            Set custom eACL                     ${USER_KEY}      ${CID}         0001000000020000000100010000000200000002000d435245415445445f45504f4348  ${AtEpoch_value_hex}  0001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}             ${EMPTY}           local_file_eacl


                            Log	                                Set eACL for Deny GET operation with StringEqual Object Extended User Header     
    ${S_OID_USER_OTH} =     Put object to NeoFS                 ${USER_KEY}      ${FILE_S}     ${CID}            ${EMPTY}            &{FILE_OTH_HEADER} 
                            Set eACL                            ${USER_KEY}      ${CID}        000100000002000000010001000000030000000200046b65793200062761626331270001000000030000 
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}      ${CID}        ${S_OID_OTHER}            ${EMPTY}           local_file_eacl
                            Get object from NeoFS               ${OTHER_KEY}      ${CID}        ${S_OID_USER}             ${EMPTY}           local_file_eacl


Generate Keys
    ${USER_KEY_GEN} =       Generate Neo private key
    ${OTHER_KEY_GEN} =      Generate Neo private key
    ${EACL_KEY_GEN} =       Form Privkey from String            782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
                            Get Neo public key                  ${EACL_KEY_GEN}
    ${SYSTEM_KEY_GEN} =	    Form Privkey from String            c428b4a06f166fde9f8afcf918194acdde35aa2612ecf42fe0c94273425ded21    


    ${SYSTEM_KEY_GEN_SN} =  Form Privkey from String            0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2


                            Set Global Variable     ${USER_KEY}                  ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}                 ${OTHER_KEY_GEN}
                            Set Global Variable     ${EACL_KEY}                  ${EACL_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}                ${SYSTEM_KEY_GEN}

                            Set Global Variable     ${SYSTEM_KEY_SN}      ${SYSTEM_KEY_GEN_SN}


Create Container Public
                            Log	                                Create Public Container
    ${PUBLIC_CID_GEN} =     Create container                    ${USER_KEY}     0x2FFFFFFF
    [Return]                ${PUBLIC_CID_GEN}
                            
 
Generate file
    # Generate small file
    ${FILE_S_GEN} =         Generate file of bytes              1024
                            Set Global Variable     ${FILE_S}          ${FILE_S_GEN}
 

Prepare eACL Role rules
                            Log	                    Set eACL for different Role cases
                            Set Global Variable     ${EACL_DENY_ALL_OTHER}        0007000000020000000100000001000000030000000000020000000300000001000000030000000000020000000200000001000000030000000000020000000500000001000000030000000000020000000400000001000000030000000000020000000600000001000000030000000000020000000700000001000000030000
                            Set Global Variable     ${EACL_ALLOW_ALL_OTHER}       0007000000010000000100000001000000030000000000010000000300000001000000030000000000010000000200000001000000030000000000010000000500000001000000030000000000010000000400000001000000030000000000010000000600000001000000030000000000010000000700000001000000030000
                            
                            Set Global Variable     ${EACL_DENY_ALL_USER}         0007000000020000000100000001000000010000000000020000000300000001000000010000000000020000000200000001000000010000000000020000000500000001000000010000000000020000000400000001000000010000000000020000000600000001000000010000000000020000000700000001000000010000
                            Set Global Variable     ${EACL_ALLOW_ALL_USER}        0007000000010000000100000001000000010000000000010000000300000001000000010000000000010000000200000001000000010000000000010000000500000001000000010000000000010000000400000001000000010000000000010000000600000001000000010000000000010000000700000001000000010000

                            Set Global Variable     ${EACL_DENY_ALL_SYSTEM}       0007000000020000000100000001000000020000000000020000000300000001000000020000000000020000000200000001000000020000000000020000000500000001000000020000000000020000000400000001000000020000000000020000000600000001000000020000000000020000000700000001000000020000
                            Set Global Variable     ${EACL_ALLOW_ALL_SYSTEM}      0007000000010000000100000001000000020000000000010000000300000001000000020000000000010000000200000001000000020000000000010000000500000001000000020000000000010000000400000001000000020000000000010000000600000001000000020000000000010000000700000001000000020000
 
                            
                            Set Global Variable     ${EACL_ALLOW_ALL_Pubkey}      000e000000010000000100000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000300000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000200000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000500000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000400000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000600000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000700000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000020000000100000001000000030000000000020000000300000001000000030000000000020000000200000001000000030000000000020000000500000001000000030000000000020000000400000001000000030000000000020000000600000001000000030000000000020000000700000001000000030000

 

Check eACL Deny and Allow All User
    Check eACL Deny and Allow All    ${USER_KEY}     ${EACL_DENY_ALL_USER}      ${EACL_ALLOW_ALL_USER}                  


Check eACL Deny and Allow All Other
    Check eACL Deny and Allow All    ${OTHER_KEY}    ${EACL_DENY_ALL_OTHER}     ${EACL_ALLOW_ALL_OTHER} 


Check eACL Deny and Allow All System
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

    # By discussion, IR can not make any operations instead of HEAD, SEARCH and GET RANGE HASH at the current moment
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY}       ${FILE_S}     ${CID}            ${EMPTY}                   &{FILE_OTH_HEADER} 
                            Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}     ${CID}            ${EMPTY}                   &{FILE_OTH_HEADER} 
                            
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Get object from NeoFS               ${SYSTEM_KEY_SN}       ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Search object                       ${SYSTEM_KEY}       ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}

                            
                            Head object                         ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}


                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256

                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY}       ${CID}        ${D_OID_USER}            ${EMPTY}
                            Delete object                       ${SYSTEM_KEY_SN}       ${CID}        ${D_OID_USER}            ${EMPTY}


                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_SYSTEM}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}
 

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY}    ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_OTH_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY_SN}    ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_OTH_HEADER} 

                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS           ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${SYSTEM_KEY}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}

                            
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_ALLOW_ALL_SYSTEM}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}


                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY}       ${FILE_S}     ${CID}            ${EMPTY}                   &{FILE_OTH_HEADER} 
                            Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}     ${CID}            ${EMPTY}                   &{FILE_OTH_HEADER} 
                            
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Get object from NeoFS               ${SYSTEM_KEY_SN}       ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl

                            Search object                       ${SYSTEM_KEY}       ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}

                            
                            Head object                         ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}


                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256

                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY}       ${CID}        ${D_OID_USER}            ${EMPTY}
                            Delete object                       ${SYSTEM_KEY_SN}       ${CID}        ${D_OID_USER}            ${EMPTY}



Check eACL Deny All Other and Allow All Pubkey

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${EACL_KEY}    ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_ALLOW_ALL_Pubkey}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${OTHER_KEY}    ${FILE_S}     ${CID}            ${EMPTY}                   &{FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${OTHER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${OTHER_KEY}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${OTHER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${OTHER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}

                            Put object to NeoFS                 ${EACL_KEY}    ${FILE_S}     ${CID}             ${EMPTY}                   &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${EACL_KEY}    ${CID}        ${S_OID_USER}             ${EMPTY}           local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}             ${EMPTY}           ${True}
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}             ${EMPTY}           0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}


Check eACL Deny and Allow All
    [Arguments]     ${KEY}       ${DENY_EACL}    ${ALLOW_EACL}

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object                       ${KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${DENY_EACL}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            ${EMPTY}            &{FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            ${True}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Set eACL                            ${USER_KEY}     ${CID}        ${ALLOW_EACL}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}


                            Put object to NeoFS                 ${KEY}    ${FILE_S}     ${CID}              ${EMPTY}            &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}            ${EMPTY}            @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            ${True}
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}       ${EMPTY}            0:256
                            Delete object                       ${KEY}    ${CID}        ${D_OID_USER}       ${EMPTY}

