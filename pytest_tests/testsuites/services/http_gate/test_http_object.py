import logging
import allure
import pytest
from container import create_container, delete_container
from file_helper import generate_file
from http_gate import (
    get_object_and_verify_hashes,
    upload_via_http_gate_curl,
    get_object_by_attr_and_verify_hashes,
    try_to_get_object_via_passed_request_and_expect_error,
    attr_into_str_header_curl
)
from python_keywords.neofs_verbs import put_object_to_random_node
from wellknown_acl import PUBLIC_ACL
from steps.cluster_test_base import ClusterTestBase
from python_keywords.neofs_verbs import delete_object

logger = logging.getLogger("NeoLogger")

@pytest.mark.sanity
@pytest.mark.http_gate
class Test_http_object(ClusterTestBase):
    PLACEMENT_RULE = "REP 2 IN X CBF 1 SELECT 4 FROM * AS X"

    @pytest.fixture(scope="class", autouse=True)
    @allure.title("[Class/Autouse]: Prepare wallet and deposit")
    def prepare_wallet(self, default_wallet):
        Test_http_object.wallet = default_wallet

    @allure.title("Test Put over gRPC, Get over HTTP")
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size"), pytest.lazy_fixture("complex_object_size")],
        ids=["simple object", "complex object"],
    )   
    def test_object_put_get_attributes(self, object_size: int):
        """
        Test that object can be put using gRPC interface and get using HTTP.

        Steps:
        1. Create object;
        2. Put objects using gRPC (neofs-cli) with attributes [--attributes chapter1=peace,chapter2=war];
        3. Download object using HTTP gate (https://github.com/nspcc-dev/neofs-http-gw#downloading);
        4. Compare hashes between original and downloaded object;
        5. [Negative] Try to the get object with specified attributes and `get` request: [get/$CID/chapter1/peace];
        6. Download the object with specified attributes and `get_by_attribute` request: [get_by_attribute/$CID/chapter1/peace];
        7. Compare hashes between original and downloaded object;
        8. [Negative] Try to the get object via `get_by_attribute` request: [get_by_attribute/$CID/$OID];


        Expected result:
        Hashes must be the same.
        """
        with allure.step("Create public container"):
            cid = create_container(
                self.wallet,
                shell=self.shell,
                endpoint=self.cluster.default_rpc_endpoint,
                rule=self.PLACEMENT_RULE,
                basic_acl=PUBLIC_ACL,
            )

        # Generate file
        file_path = generate_file(object_size)

        # List of Key=Value attributes
        obj_key1 = "chapter1"
        obj_value1 = "peace"
        obj_key2 = "chapter2"
        obj_value2 = "war"

        # Prepare for grpc PUT request
        key_value1 = obj_key1 + "=" + obj_value1
        key_value2 = obj_key2 + "=" + obj_value2

        with allure.step("Put objects using gRPC [--attributes chapter1=peace,chapter2=war]"):
            oid = put_object_to_random_node(
                wallet=self.wallet,
                path=file_path,
                cid=cid,
                shell=self.shell,
                cluster=self.cluster,
                attributes=f"{key_value1},{key_value2}",
            )
        with allure.step("Get object and verify hashes [ get/$CID/$OID ]"):
            get_object_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                wallet=self.wallet,
                cid=cid,
                shell=self.shell,
                nodes=self.cluster.storage_nodes,
                endpoint=self.cluster.default_http_gate_endpoint,
            )
        with allure.step("[Negative] try to get object: [get/$CID/chapter1/peace]"):
            attrs = {obj_key1: obj_value1, obj_key2: obj_value2}
            request = f"/get/{cid}/{obj_key1}/{obj_value1}"
            expected_err_msg = "Failed to get object via HTTP gate:"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid,
                oid=oid,
                error_pattern=expected_err_msg,
                http_request_path=request,
                attrs=attrs,
                endpoint=self.cluster.default_http_gate_endpoint,
            )

        with allure.step(
            "Download the object with attribute [get_by_attribute/$CID/chapter1/peace]"
        ):
            get_object_by_attr_and_verify_hashes(
                oid=oid,
                file_name=file_path,
                cid=cid,
                attrs=attrs,
                endpoint=self.cluster.default_http_gate_endpoint,
            )
        with allure.step("[Negative] try to get object: get_by_attribute/$CID/$OID"):
            request = f"/get_by_attribute/{cid}/{oid}"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid,
                oid=oid,
                error_pattern=expected_err_msg,
                http_request_path=request,
                endpoint=self.cluster.default_http_gate_endpoint,
            )
    @allure.title("Test Put/Get with similar attributes")
    @pytest.mark.parametrize(
        "object_size",
        [pytest.lazy_fixture("simple_object_size"), pytest.lazy_fixture("complex_object_size")],
        ids=["simple object", "complex object"],
    )    
    def test_put_get_user_headers(self, object_size: int):
        """
        Test to put objects with a bit similar attributes and try to get them with corresponding attrs

        Steps:
        1. Create public container
        2. Allocate and put object#1 via http with attributes: [Writer=Leo Tolstoy, Chapter1=peace, Chapter2=w@r]
        3. Allocate and put object#2 via http with attributes: [Writer=Leo Tolstoy, Ch@pter1=peace, chapter2=w@r]
        4. Download object#1 with attributes [Chapter2=w@r] and compare hashes
        5. Download object#2 with attributes [chapter2=w@r] and compare hashes
        6. Download object#2 with attributes [Ch@pter1=peace] and compare hashes
        7. Delete object#2
        8. Download object#1 with attributes [Writer=Leo Tolstoy] and compare hashes
        9. [Negative] Allocate and attemt to put object#3 via http with attributes: [Writer=Leo Tolstoy, Writer=peace, peace=peace]
            Expected: "Error duplication of attributes detected"
        10. Delete container
        11. [Negative] Try to download object with attributes [peace=peace]
            Expected: "HTTP request sent, awaiting response... 404 Not Found"
        """
        with allure.step("1. Create public container"):
            cid = create_container(
                self.wallet,
                shell=self.shell,
                endpoint=self.cluster.default_rpc_endpoint,
                rule=self.PLACEMENT_RULE,
                basic_acl=PUBLIC_ACL,
            )       
        with allure.step("2. Allocate and put object#1 via http with attributes: [Writer=Leo Tolstoy, Chapter1=peace, Chapter2=w@r]"):
            file_path_1 = generate_file(object_size)
            attrs_obj1 = {"Writer": "Leo Tolstoy", "Chapter1": "peace", "Chapter2": "w@r"}           
            headers = attr_into_str_header_curl(attrs_obj1) 
            oid1_curl = upload_via_http_gate_curl(
                cid=cid,
                filepath=file_path_1,
                large_object=True,
                endpoint=self.cluster.default_http_gate_endpoint,
                headers=headers               
            )
        with allure.step("3. Allocate and put object#2 via http with attributes: [Writer=Leo Tolstoy, Ch@pter1=peace, chapter2=w@r]"):       
            file_path_2 = generate_file(object_size)
            attrs_obj2 = {"Writer": "Leo Tolstoy", "Ch@pter1": "peace", "chapter2": "w@r"}           
            headers = attr_into_str_header_curl(attrs_obj2) 
            oid2_curl = upload_via_http_gate_curl(
                cid=cid,
                filepath=file_path_2,
                large_object=True,
                endpoint=self.cluster.default_http_gate_endpoint, 
                headers=headers              
            )      
        with allure.step("4. Download object#1 via wget with attributes [Chapter2=w@r] and compare hashes"):
            key_value_pair = {"Chapter2" : attrs_obj1["Chapter2"]}
            get_object_by_attr_and_verify_hashes(
                oid=oid1_curl,
                file_name=file_path_1,
                cid=cid,
                attrs=key_value_pair,
                endpoint=self.cluster.default_http_gate_endpoint
            )
        with allure.step("5. Download object#2 via wget with attributes [chapter2=w@r] and compare hashes"): 
            key_value_pair = {"chapter2" : attrs_obj2["chapter2"]}
            get_object_by_attr_and_verify_hashes(
                oid=oid2_curl,
                file_name=file_path_2,
                cid=cid,
                attrs=key_value_pair,
                endpoint=self.cluster.default_http_gate_endpoint          
            )       
        with allure.step("6. Download object#2 via wget with attributes [Ch@pter1=peace] and compare hashes"): 
            key_value_pair = {"Ch@pter1" : attrs_obj2["Ch@pter1"]}
            get_object_by_attr_and_verify_hashes(
                oid=oid2_curl,
                file_name=file_path_2,
                cid=cid,
                attrs=key_value_pair,
                endpoint=self.cluster.default_http_gate_endpoint          
            )        
        with allure.step("7. Delete object#2"):                         
            delete_object(
                wallet=self.wallet,
                cid=cid,
                oid=oid2_curl,
                shell=self.shell,
                endpoint=self.cluster.default_rpc_endpoint,
            )            
        with allure.step("8. Download object#1 with attributes [Writer=Leo Tolstoy] and compare hashes"):
            key_value_pair = {"Writer" : attrs_obj1["Writer"]}
            get_object_by_attr_and_verify_hashes(
                oid=oid1_curl,
                file_name=file_path_1,
                cid=cid,
                attrs=key_value_pair,
                endpoint=self.cluster.default_http_gate_endpoint, 
            )        
        with allure.step("9. [Negative] Allocate and attemt to put object#3 via http with attributes: [Writer=Leo Tolstoy, Writer=peace, peace=peace]"):   
            file_path_3 = generate_file(object_size)
            attrs_obj3 = {"Writer": "Leo Tolstoy", "peace": "peace"}           
            headers = attr_into_str_header_curl(attrs_obj3)                    
            headers.append(" ".join(attr_into_str_header_curl({"Writer": "peace"})))   
            error_pattern = f"key duplication error: X-Attribute-Writer"
            upload_via_http_gate_curl(
                cid=cid,
                filepath=file_path_3, 
                large_object=True,
                endpoint=self.cluster.default_http_gate_endpoint, 
                headers=headers,
                error_pattern=error_pattern         
            )

        with allure.step("10. Delete container"):
            delete_container(
                wallet=self.wallet,
                cid=cid,
                shell=self.shell,
                endpoint=self.cluster.default_rpc_endpoint,
            )       
        with allure.step("11. [Negative] Try to download (wget) object via wget with attributes [peace=peace]"):     
            request = f"/get/{cid}/peace/peace"                
            error_pattern = "404 Not Found"
            try_to_get_object_via_passed_request_and_expect_error(
                cid=cid,
                oid="",
                error_pattern=error_pattern,
                attrs=attrs_obj3,
                http_request_path=request,
                endpoint=self.cluster.default_http_gate_endpoint,
            )    
            