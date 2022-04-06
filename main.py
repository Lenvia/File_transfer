from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
import json

es = Elasticsearch()


def getData(path="dureader_retrieval-data/passage_collection.json", total=100):
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for i in range(total):
            line = json.loads(f.readline())
            para_id = line['paragraph_id']
            para_text = line['paragraph_text']
            lines.append((para_id, para_text))
    return lines


def deleteInices(my_index):
    if True and es.indices.exists(my_index):  # 确认删除再改为True
        print("删除之前存在的")
        es.indices.delete(index=my_index)


def createIndex(my_index, my_doc):
    # index settings
    request_body = {
        "mappings": {
            "properties": {
                "paragraph_id": {
                    "type": "keyword"
                },
                "paragraph_text": {
                    "type": "text"
                }
            }

        }
    }

    es.indices.create(index=my_index, body=request_body)
    print("创建index成功！")


def insertData(lines, my_index, my_doc, one_bulk):
    # 插入数据
    # one_bulk表示一个bulk里装多少个
    body = []
    body_count = 0  # 记录body里面有多少个.
    # 最后一个bulk可能没满one_bulk,但也要插入

    print("共需要插入%d条..." % len(lines))
    pbar = tqdm(total=len(lines))

    for id, text in lines:
        data1 = {"paragraph_id": id,
                 "paragraph_text": text}
        # print(data1)
        every_body = \
            {
                "_index": my_index,
                # "_type": my_doc,
                "_source": data1
            }

        if body_count < one_bulk:
            body.append(every_body)
            body_count += 1
        else:
            helpers.bulk(es, body)  # 还是要用bulk啊，不然太慢了
            pbar.update(one_bulk)
            body_count = 0
            body = []
            body.append(every_body)
            body_count += 1

    if len(body) > 0:
        # 如果body里面还有，则再插入一次（最后非整块的）
        helpers.bulk(es, body)
        # pbar.update(len(body))
        print('done')

    pbar.close()
    # res = es.index(index=my_index,doc_type=my_doc,id=my_key_id,body=data1)  #一条插入
    print("插入数据完成!")


def keywordSearch(keywords1, my_index, my_doc):
    # 根据keywords1来查找，倒排索引
    my_search1 = \
        {
            "query": {
                "match": {
                    "paragraph_text": keywords1
                }
            }
        }
    # 直接查询
    # res= es.search(index=my_index,body=my_search1)
    # total = res["hits"]["total"] #一共这么多个
    # print("共查询到%d条数据"%total)

    # helpers查询
    es_result = helpers.scan(
        client=es,
        query=my_search1,
        scroll='10m',
        index=my_index,
        timeout='10m'
    )
    es_result = [item for item in es_result]  # 原始是生成器<generator object scan at 0x0000022E384697D8>
    # print(es_result)  # 你可以直接打印查看
    search_res = []

    # 取前十条
    es_result = es_result[:10]
    for item in es_result:
        tmp = item['_source']
        search_res.append((tmp['paragraph_id'], tmp['paragraph_text']))
    print("共查询到%d条数据" % len(es_result))

    return search_res


if __name__ == '__main__':
    my_index = "baidu"
    my_doc = "my_doc"
    # 删除已有索引
    # deleteInices(my_index)
    # 创建索引
    # createIndex(my_index, my_doc)

    # 获取并插入数据
    # lines = getData(total=100)
    # insertData(lines, my_index, my_doc, one_bulk=5000)

    results = keywordSearch("怎么屏蔽qq新闻弹窗", my_index, my_doc)
    for res in results:
        print(res)
