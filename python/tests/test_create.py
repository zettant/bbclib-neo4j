import sys
sys.path.append("../")

import bbclib_neo4j
import bbclib


db = None
user_id1 = bbclib.bbclib_utils.get_new_id("user_id1", include_timestamp=False)
user_id2 = bbclib.bbclib_utils.get_new_id("user_id2", include_timestamp=False)
asset_group_id1 = bbclib.bbclib_utils.get_new_id("asset_group_id1", include_timestamp=False)
asset_group_id2 = bbclib.bbclib_utils.get_new_id("asset_group_id2", include_timestamp=False)
domain_id = bbclib.bbclib_utils.get_new_id("domain_id", include_timestamp=False)

keypair1 = bbclib.KeyPair()
keypair2 = bbclib.KeyPair()

txobj = None


class TestBBclibNeo4jCreate(object):

    def test_01_make_asset_and_create_1(self):
        print("\n-----", sys._getframe().f_code.co_name, "-----")
        relation1 = {
            "asset_group_id": asset_group_id1.hex(),
            "user_id": user_id1.hex(),
            "asset_body": {
                "astname": "aaaaaaa",
                "kind": "type1"
            }
        }
        global txobj
        txobj, sql = bbclib_neo4j.create([relation1], [user_id1])
        assert len(txobj.relations) == 1
        #print(txobj)
        assert sql != ""
        #print(sql)

    def test_02_make_asset_and_create_2(self):
        print("\n-----", sys._getframe().f_code.co_name, "-----")
        global txobj
        relation1 = {
            "asset_group_id": asset_group_id1.hex(),
            "user_id": user_id1.hex(),
            "asset_body": {
                "astname": "aaaaaaa",
                "kind": "type1"
            },
            "pointers": [{
                "transaction_id": txobj.transaction_id.hex(),
                "asset_id": txobj.relations[0].asset.asset_id.hex()
            }]
        }
        relation2 = {
            "asset_group_id": asset_group_id2.hex(),
            "user_id": user_id2.hex(),
            "asset_body": {
                "astname": "bbbbbbbbb",
                "kind": "type2"
            },
            "pointers": [{
                "transaction_id": txobj.transaction_id.hex(),
                "asset_id": txobj.relations[0].asset.asset_id.hex()
            }]
        }
        txobj, sql = bbclib_neo4j.create([relation1, relation2], [user_id1, user_id2])
        assert len(txobj.relations) == 2
        assert len(txobj.relations[0].pointers) == 1
        assert len(txobj.relations[1].pointers) == 1
        #print(txobj)
        assert sql != ""
        #print(sql)

