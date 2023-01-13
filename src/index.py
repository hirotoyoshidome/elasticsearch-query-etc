from elasticsearch import Elasticsearch, helpers


# consts.
HOST: str = "eqe_elasticsearch"
USER: str = "eqe"
PASSWORD: str = "password"
PORT: int = 9200
INDEX: str = "eqe"

ELASTICSEARCH_MAX_RESULT: int = 999999


def get_client() -> Elasticsearch:
    url = "http://{user}:{password}@{host}:{port}"
    nodes = [url.format(user=USER, password=PASSWORD, host=HOST, port=PORT)]
    return Elasticsearch(
        nodes,
        timeout=600,
        ca_certs=False,
        verify_certs=False,
        ssl_show_warn=False,
    )


def create_index(es) -> None:
    es.indices.create(
        index=INDEX,
        body={
            "settings": {
                "mapping": {"nested_objects": {"limit": ELASTICSEARCH_MAX_RESULT}},
                "max_result_window": ELASTICSEARCH_MAX_RESULT,
            },
            "mappings": {
                "properties": {
                    "id": {
                        "type": "integer",
                    },
                    "name": {
                        "type": "keyword",
                    },
                    "age": {
                        "type": "integer",
                    },
                    "pets": {
                        "type": "nested",
                        "properties": {
                            "type": {"type": "keyword"},
                            "gender": {"type": "integer"},
                        },
                    },
                }
            },
        },
        request_timeout=1800,  # 30m
    )

    sample_data = _get_sample_data()
    data = []
    for d in sample_data:
        data.append(
            {"_index": INDEX, "_id": d["id"], "_op_type": "create", "_source": d}
        )

    if len(data) > 0:
        helpers.bulk(es, data)
        data.clear()


def _get_sample_data() -> list:
    return [
        {
            "id": 1,
            "name": "太郎",
            "age": 20,
            "pets": [
                {"type": "dog", "gender": "1"},
                {"type": "dog", "gender": "2"},
            ],
        },
        {
            "id": 2,
            "name": "次郎",
            "age": 25,
            "pets": [
                {"type": "cat", "gender": "1"},
            ],
        },
        {"id": 3, "name": "花子", "age": 30, "pets": []},
    ]


def search_sample(es) -> None:
    query = {
        "query": {
            "bool": {
                "should": [
                    {"term": {"name": "太郎"}},
                    {"term": {"name": "次郎"}},
                ],
                "minimum_should_match": 1,
            }
        },
        "fields": [
            "id",
            "name",
            "age",
            "pets.type",
            "pets.gender",
        ],
        "_source": False,
    }
    res = es.search(
        index=INDEX,
        body=query,
        size=999,
    )

    for r in res["hits"]["hits"]:
        obj = r["fields"]
        print(obj)


def main() -> None:
    es = get_client()

    # create_index(es)

    search_sample(es)


if __name__ == "__main__":
    main()
