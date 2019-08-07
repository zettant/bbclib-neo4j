Neo4j adaptor for py-bbclib
======

# How to test
1. run docker of Neo4j container
```bash
bash ./start_docker.sh
```

2. Exec test
```bash
. venv/bin/activate
cd tests
pytest test_insert_transactions.py
```

3. Access web site on the docker

http://127.0.0.1:7474/browser/

4. Run the following query
```
MATCH (n) RETURN (n)
```
