*** Variables ***

${ACL_TEST_FILES} =     robot/resources/files/eacl_tables

${EACL_DENY_ALL_OTHERS} =      ${ACL_TEST_FILES}/gen_eacl_deny_all_OTHERS
${EACL_ALLOW_ALL_OTHERS} =     ${ACL_TEST_FILES}/gen_eacl_allow_all_OTHERS

${EACL_DENY_ALL_USER} =       ${ACL_TEST_FILES}/gen_eacl_deny_all_USER
${EACL_ALLOW_ALL_USER} =      ${ACL_TEST_FILES}/gen_eacl_allow_all_USER

${EACL_DENY_ALL_SYSTEM} =     ${ACL_TEST_FILES}/gen_eacl_deny_all_SYSTEM
${EACL_ALLOW_ALL_SYSTEM} =    ${ACL_TEST_FILES}/gen_eacl_allow_all_SYSTEM

${EACL_ALLOW_ALL_Pubkey} =    ${ACL_TEST_FILES}/gen_eacl_allow_pubkey_deny_OTHERS

${EACL_COMPOUND_GET_OTHERS} =    ${ACL_TEST_FILES}/gen_eacl_compound_get_OTHERS
${EACL_COMPOUND_GET_USER} =      ${ACL_TEST_FILES}/gen_eacl_compound_get_USER
${EACL_COMPOUND_GET_SYSTEM} =    ${ACL_TEST_FILES}/gen_eacl_compound_get_SYSTEM

${EACL_COMPOUND_DELETE_OTHERS} =    ${ACL_TEST_FILES}/gen_eacl_compound_del_OTHERS
${EACL_COMPOUND_DELETE_USER} =      ${ACL_TEST_FILES}/gen_eacl_compound_del_USER
${EACL_COMPOUND_DELETE_SYSTEM} =    ${ACL_TEST_FILES}/gen_eacl_compound_del_SYSTEM

${EACL_COMPOUND_GET_HASH_OTHERS} =    ${ACL_TEST_FILES}/gen_eacl_compound_get_hash_OTHERS
${EACL_COMPOUND_GET_HASH_USER} =      ${ACL_TEST_FILES}/gen_eacl_compound_get_hash_USER
${EACL_COMPOUND_GET_HASH_SYSTEM} =    ${ACL_TEST_FILES}/gen_eacl_compound_get_hash_SYSTEM

${EACL_XHEADER_DENY_ALL} =     ${ACL_TEST_FILES}/gen_eacl_xheader_deny_all
${EACL_XHEADER_ALLOW_ALL} =     ${ACL_TEST_FILES}/gen_eacl_xheader_allow_all
