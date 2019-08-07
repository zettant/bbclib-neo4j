from neo4j import GraphDatabase
import bbclib
import itertools


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

    def insert(self, txobj, asset_body_sql=None, is_serialized=False):
        """Insert transaction object into graph DB
        Args:
            txobj (bbclib.BBcTransaction): transaction object to insert
            asset_body_sql (string): partial string of sql for asset_body
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
                sql = ""
                txid = txobj.transaction_id.hex()
                timestamp = txobj.timestamp
                for i, rtn in enumerate(txobj.relations):
                    asset_group_id = rtn.asset_group_id.hex()
                    user_id = rtn.asset.user_id.hex()
                    asset_id = rtn.asset.asset_id.hex()
                    asset_body = ""
                    if asset_body_sql is not None:
                        asset_body = ", " + asset_body_sql
                    sql += " CREATE (this%d:asset {asset_id:\"%s\", transaction_id:\"%s\", timestamp:%d, asset_group_id:\"%s\", user_id:\"%s\" %s}) " % (i, asset_id, txid, timestamp, asset_group_id, user_id, asset_body)
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
                try:
                    tx.success = True
                    tx.run(sql)
                except:
                    tx.success = False
                    return False
        return True

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
