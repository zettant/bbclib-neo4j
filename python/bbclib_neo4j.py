from neo4j import GraphDatabase
import bbclib
import itertools
import binascii
import json


def _get_cypher_ql(txobj):
    """Create a query string of Cypher QL for the transaction object
    Args:
        txobj (bbclib.BBcTransaction): transaction object
    Returns:
        string: query string
    """
    sql = ""
    txid = txobj.transaction_id.hex()
    timestamp = txobj.timestamp
    for i, rtn in enumerate(txobj.relations):
        asset_group_id = rtn.asset_group_id.hex()
        user_id = rtn.asset.user_id.hex()
        asset_id = rtn.asset.asset_id.hex()
        body = rtn.asset.asset_body
        if isinstance(body, bytes) or isinstance(body, bytearray):
            body = json.loads(body.decode())
        elif isinstance(body, str):
            body = json.loads(body)
        asset_body = list()
        for k, v in body.items():
            asset_body.append("%s: \"%s\"" % (k, v))
        sql += " CREATE (this%d:asset {asset_id:\"%s\", transaction_id:\"%s\", timestamp:%d, asset_group_id:\"%s\", user_id:\"%s\", %s}) " % (i, asset_id, txid, timestamp, asset_group_id, user_id, ",".join(asset_body))
        pointers = list()
        pointer_edges = list()
        if rtn.pointers is not None:
            for j, ptr in enumerate(rtn.pointers):
                if ptr.asset_id is not None:
                    pointers.append("(ptr%d:asset {asset_id:\"%s\"})" % (j, ptr.asset_id.hex()))
                    pointer_edges.append(" MERGE (this%d)-[:pointer]->(ptr%d) " % (i, j))
                elif ptr.transaction_id is not None:
                    pointers.append("(ptr%d:asset {transaction_id:\"%s\"})" % (j, ptr.transaction_id.hex()))
                    pointer_edges.append(" MERGE (this%d)-[:pointer]->(ptr%d) " % (i, j))
        if len(pointers) > 0:
            sql = " MATCH " + ",".join(pointers) + " " + sql + " ".join(pointer_edges)

    for array in itertools.combinations(range(len(txobj.relations)), 2):
        sql += " CREATE (this%d)-[:transaction]->(this%d) " % (array[0], array[1])
        sql += " CREATE (this%d)-[:transaction]->(this%d)" % (array[1], array[0])

    # TODO: Need to support BBcEvent and BBcReference
    return sql


def create(asset_info_list, witness_users=None):
    """Create transaction object and query string for insertion into graph DB
    Args:
        asset_info_list (list({user_id, asset_group_id, asset_body, pointers)): list of asset information
        witness_users (list(bytes)): list of user_ids (binary)
    Returns:
        bbclib.BBcTransaction: transaction object without BBcWitness and BBcSignature
        string: Cypher QL query string for inserting the transaction
    """
    txobj = bbclib.BBcTransaction()
    for rtn in asset_info_list:
        if "user_id" not in rtn or "asset_group_id" not in rtn or "asset_body" not in rtn:
            print("Invalid request")
            return None, ""
        relation = bbclib.BBcRelation(asset_group_id=binascii.a2b_hex(rtn["asset_group_id"]))
        if isinstance(rtn["asset_body"], dict):
            relation.add(asset=bbclib.BBcAsset(user_id=binascii.a2b_hex(rtn["user_id"]), asset_body=json.dumps(rtn["asset_body"])))
        elif isinstance(rtn["asset_body"], bytes) or isinstance(rtn["asset_body"], bytearray):
            relation.add(asset=bbclib.BBcAsset(user_id=binascii.a2b_hex(rtn["user_id"]), asset_body=rtn["asset_body"].decode()))
        else:
            relation.add(asset=bbclib.BBcAsset(user_id=binascii.a2b_hex(rtn["user_id"]), asset_body=rtn["asset_body"]))
        if "pointers" in rtn:
            for ptr in rtn["pointers"]:
                if "asset_id" not in ptr and "transaction_id" not in ptr:
                    print("Invalid request")
                    return None, ""
                relation.add(pointer=bbclib.BBcPointer(transaction_id=binascii.a2b_hex(ptr.get("transaction_id", None)),
                                                       asset_id=binascii.a2b_hex(ptr.get("asset_id", None))))
        txobj.add(relation=relation)
    if witness_users is not None:
        txobj.add(witness=bbclib.BBcWitness())
        for u in witness_users:
            txobj.witness.add_witness(user_id=u)

    txobj.digest()
    return txobj, _get_cypher_ql(txobj)


class Neo4jAdapter(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def run(self, sql):
        """Run sql simply
        Args:
            sql (string): Cypher QL
        """
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                tx.run(sql)

    def search(self, sql):
        """Search assets
        Args:
            sql (string): Cypher QL
        Returns:

        """
        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                result = tx.run(sql)
                return result

    def insert(self, txobj, is_serialized=False):
        """Insert transaction object into graph DB
        Args:
            txobj (bbclib.BBcTransaction): transaction object to insert
            is_serialized (bool): True if given txobj is serialized data
        Returns:
            bool: True if succeeded
        """
        if is_serialized:
            txobj, _ = bbclib.deserialize(txobj)
        if not bbclib.validate_transaction_object(txobj):
            return False

        with self._driver.session() as session:
            with session.begin_transaction() as tx:
                sql = _get_cypher_ql(txobj)
                try:
                    tx.success = True
                    tx.run(sql)
                except:
                    tx.success = False
                    return False
        return True
