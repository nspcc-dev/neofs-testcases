*** Settings ***
Variables   ../../variables/common.py

 
Library     ${RESOURCES}/environment.py
Library     ${RESOURCES}/neo.py
Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment.py
Library     ${RESOURCES}/assertions.py
Library     ${RESOURCES}/neo.py


*** Variables ***
&{FILE_USR_HEADER} =        key1=1    key2='abc1'
&{FILE_USR_HEADER_DEL} =    key1=del  key2=del
&{FILE_OTH_HEADER} =        key1=oth  key2=oth

*** Test cases ***
Basic ACL Operations
    [Documentation]     Testcase to validate NeoFS operations with extended ACL.
    [Tags]              ACL  NeoFS  NeoCLI
    [Timeout]           20 min

    Generate Keys
    Generate file
    Prepare eACL rules
 
#    Check Filters
    Check Actions
    
 

 
*** Keywords ***

Check Actions
    Check eACL Deny and Allow All Other
    Check eACL Deny and Allow All User
    Check eACL Deny and Allow All System

    Check eACL Deny All Other and Allow All Pubkey

    


Check Filters
    Check eACL MatchType String

    

Check eACL MatchType String
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}      ${FILE_S}     ${CID}            &{FILE_USR_HEADER} 
    ${HEADER} =             Head object                         ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}     ${True}
                            Get nodes with object               ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}
                            Parse Object Header                 ${HEADER} 
                            

#### Format
#
#{
#  "Records": [
#    {
#      "Operation": OPERATION,
#      "Action": ACTION,
#      "Filters": [
#        {
#          "HeaderType": HEADER_TYPE,
#          "MatchType": MATCH_TYPE,
#          "Name": {HeaderType = ObjectSystem ? SYSTEM_HEADER : ANY_STRING},
#          "Value": ANY_STRING,
#        }
#      ],
#      "Targets": [
#        {
#          "Role": ROLE,
#          "Keys": BASE64_STRING[...]
#        }
#      ]
#    }
#  ]
#}


# * ANY_STRING - any JSON string value
# * BASE64_STRING - any Base64 string (RFC 4648)
# * ACTION - string, one of
#   * Deny
#   * Allow


# * ROLE - string, one of
#   * User
#   * System
#   * Others
#   * Pubkey
# * OPERATION - string, one of
#   * GET
#   * HEAD
#   * PUT
#   * DELETE
#   * SEARCH
#   * GETRANGE
#   * GETRANGEHASH



# * HEADER_TYPE - string, one of
#   * Request
#   * ObjectSystem
#   * ObjectUser


# * MATCH_TYPE - string, one of
#   * StringEqual
#   * StringNotEqual


# * SYSTEM_HEADER - string one of
#   * ID
#   * CID
#   * OWNER_ID
#   * VERSION
#   * PAYLOAD_LENGTH
#   * CREATED_UNIX
#   * CREATED_EPOCH
#   * LINK_PREV
#   * LINK_NEXT
#   * LINK_CHILD
#   * LINK_PAR
#   * LINK_SG






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
    ${FILE_S_HASH_GEN} =    Get file hash                       ${FILE_S_GEN}

                            Set Global Variable     ${FILE_S}          ${FILE_S_GEN}
                            Set Global Variable     ${FILE_S_HASH}     ${FILE_S_HASH_GEN}

Prepare eACL rules
                            Log	                    Set eACL for different cases
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
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            &{FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            &{FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

    # By discussion, IR can not make any operations instead of HEAD, SEARCH and GET RANGE HASH at the current moment
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY}       ${FILE_S}     ${CID}                   &{FILE_OTH_HEADER} 
                            Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}     ${CID}                   &{FILE_OTH_HEADER} 
                            
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            local_file_eacl

                            Get object from NeoFS               ${SYSTEM_KEY_SN}       ${CID}        ${S_OID_USER}            local_file_eacl

                            Search object                       ${SYSTEM_KEY}       ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}

                            
                            Head object                         ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${True}
                            Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${True}


                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            0:256

                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY}       ${CID}        ${D_OID_USER}
                            Delete object                       ${SYSTEM_KEY_SN}       ${CID}        ${D_OID_USER}


                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_SYSTEM}
                            Sleep                               30sec
 


                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY}    ${FILE_S}            ${CID}            &{FILE_OTH_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY_SN}    ${FILE_S}            ${CID}            &{FILE_OTH_HEADER} 

                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS           ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            local_file_eacl
                            
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${SYSTEM_KEY}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}

                            
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            ${True}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${True}
                            
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            0:256
                            
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY}    ${CID}        ${S_OID_USER}
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}


                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_ALLOW_ALL_SYSTEM}
                            Sleep                               30sec


                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY}       ${FILE_S}     ${CID}                   &{FILE_OTH_HEADER} 
                            Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}     ${CID}                   &{FILE_OTH_HEADER} 
                            
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            local_file_eacl

                            Get object from NeoFS               ${SYSTEM_KEY_SN}       ${CID}        ${S_OID_USER}            local_file_eacl

                            Search object                       ${SYSTEM_KEY}       ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Search object                       ${SYSTEM_KEY_SN}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}

                            
                            Head object                         ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            ${True}
                            Head object                         ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            ${True}


                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY}       ${CID}        ${S_OID_USER}            0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${CID}        ${S_OID_USER}            0:256

                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY}       ${CID}        ${D_OID_USER}
                            Delete object                       ${SYSTEM_KEY_SN}       ${CID}        ${D_OID_USER}





Check eACL Deny All Other and Allow All Pubkey

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            &{FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            &{FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${EACL_KEY}    ${FILE_S}            ${CID}            &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${EACL_KEY}    ${CID}        ${S_OID_USER}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${True}
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}

                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_ALLOW_ALL_Pubkey}
                            Sleep                               30sec
 


                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${OTHER_KEY}    ${FILE_S}            ${CID}            &{FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${OTHER_KEY}    ${CID}        ${S_OID_USER}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${OTHER_KEY}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${OTHER_KEY}    ${CID}        ${S_OID_USER}            ${True}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${OTHER_KEY}    ${CID}        ${S_OID_USER}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}

                            Put object to NeoFS                 ${EACL_KEY}    ${FILE_S}     ${CID}            &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${EACL_KEY}    ${CID}        ${S_OID_USER}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${True}
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}




Check eACL Deny and Allow All
    [Arguments]     ${KEY}       ${DENY_EACL}    ${ALLOW_EACL}

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            &{FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            &{FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

                            Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${True}
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            0:256
                            Delete object                       ${KEY}    ${CID}        ${D_OID_USER}

                            Set eACL                            ${USER_KEY}     ${CID}        ${DENY_EACL}
                            Sleep                               30sec

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            &{FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${KEY}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${True}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${KEY}    ${CID}        ${S_OID_USER}


                            Set eACL                            ${USER_KEY}     ${CID}        ${ALLOW_EACL}
                            Sleep                               30sec


                            Put object to NeoFS                 ${KEY}    ${FILE_S}            ${CID}            &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${KEY}    ${CID}        ${S_OID_USER}            local_file_eacl
                            Search object                       ${KEY}    ${CID}        ${EMPTY}                 @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${KEY}    ${CID}        ${S_OID_USER}            ${True}
                            Get Range                           ${KEY}    ${CID}        ${S_OID_USER}            0:256
                            Delete object                       ${KEY}    ${CID}        ${D_OID_USER}







 


 



#  docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 13a75c3bc71865ef9474f314dedb7aa9e2b22048a86bd431578abc30971f319a container set-eacl --cid 8PD2SdxUB1P6122mHP14XcRkQtWg2XPHaeDysWKz3ARy --eacl 0a4b080210021a1e080310011a0a686561646572206b6579220c6865616465722076616c7565222508031221031a6c6fbbdf02ca351745fa86b9ba5a9452d785ac4f7fc2b7548ca2a46c4fcf4a
#  docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 13a75c3bc71865ef9474f314dedb7aa9e2b22048a86bd431578abc30971f319a container set-eacl --cid 8PD2SdxUB1P6122mHP14XcRkQtWg2XPHaeDysWKz3ARy --eacl 0a4a080210021a1e080310011a0a686561646572206b6579220c6865616465722076616c75652224080312200eef0860d2f81ed724ee45e7275a6a917791503582202c47459804192e1ba04a

#  docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 13a75c3bc71865ef9474f314dedb7aa9e2b22048a86bd431578abc30971f319a container get-eacl --cid 8PD2SdxUB1P6122mHP14XcRkQtWg2XPHaeDysWKz3ARy 





########################################
########################################

Create Containers DELETE 
    # Create containers:

                            Log	                                Create Private Container
    ${INCOR_CID_GEN} =      Create container                    ${USER_KEY}     0x3FFFFFFF
                            Container Existing                  ${USER_KEY}     ${INCOR_CID_GEN}  

 

                            Log	                                Create Private Container
    ${PRIV_CID_GEN} =       Create container                    ${USER_KEY}     0x0C8C8CCC
                            Container Existing                  ${USER_KEY}     ${PRIV_CID_GEN}  

                            Log	                                Create None Container
    ${NONE_CID_GEN} =       Create container                    ${USER_KEY}     0x2000000
                            Container Existing                  ${USER_KEY}     ${NONE_CID_GEN}    

    Set Global Variable     ${INCOR_CID}         ${INCOR_CID_GEN}
    Set Global Variable     ${PUBLIC_CID}        ${PUBLIC_CID_GEN}
    Set Global Variable     ${PRIV_CID}          ${PRIV_CID_GEN}
    Set Global Variable     ${NONE_CID}          ${NONE_CID_GEN}