import json
from datetime import datetime
from django.shortcuts import render
from django.views.generic.base import View
from search.models import ArticleType
from django.http import HttpResponse
from elasticsearch import Elasticsearch
import redis
client = Elasticsearch(hosts=["localhost"])
redis_cli = redis.StrictRedis()


class IndexView(View):
    # 首页热门搜索
    def get(self, request):
        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=5)
        return render(request, "index.html", {"topn_search": topn_search})


class SearchSuggest(View):
    def get(self, request):
        key_words = request.GET.get('s', '')
        re_datas = []
        if key_words:
            s = ArticleType.search()
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest", "fuzzy": {
                    "fuzziness": 2
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                source = match._source
                re_datas.append(source["title"])

        return HttpResponse(json.dumps(re_datas), content_type="application/json")


class SearchView(View):
    def get(self, request):
        key_words = request.GET.get('q', '')
        redis_cli.zincrby("search_keywords_set", key_words)
        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=5)
        page = request.GET.get('p', '1')
        # s_type = request.GET.get('s', "article")
        s_type = request.GET.get('s_type', '')
        try:
            page = int(page)
        except:
            page = 1
        hit_list = []
        start_time = datetime.now()

        if s_type == 'article':
            # 文章
            response = client.search(
                index="jobbole",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["tags", "title", "content"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "content": {}
                        }
                    }
                }
            )
            end_time = datetime.now()
            last_seconds = (end_time - start_time).total_seconds()
            total_nums = response["hits"]["total"]
            if (page % 10) > 0:
                page_nums = int(total_nums / 10) + 1
            else:
                page_nums = int(total_nums / 10)

            for hit in response["hits"]["hits"]:
                hit_dict = {}
                if "title" in hit["highlight"]:
                    hit_dict["title"] = "".join(hit["highlight"]["title"])
                else:
                    hit_dict["title"] = hit["_source"]["title"]
                if "content" in hit["highlight"]:
                    hit_dict["content"] = "".join(hit["highlight"]["content"])[:500]
                else:
                    hit_dict["content"] = hit["_source"]["content"][:500]

                hit_dict["create_date"] = hit["_source"]["create_date"]
                hit_dict["url"] = hit["_source"]["url"]
                hit_dict["score"] = hit["_score"]
                hit_dict["website"] = "伯乐在线"
                hit_list.append(hit_dict)
        elif s_type == 'position':
            # 职位
            response = client.search(
                index="lagou",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["title", "tags", "job_desc", "job_advantage",
                                       "company_name", "job_addr", "job_city", "degree_need"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "job_desc": {},
                            "company_name": {}
                        }
                    }
                }
            )
            end_time = datetime.now()
            last_seconds = (end_time - start_time).total_seconds()
            total_nums = response["hits"]["total"]
            if (page % 10) > 0:
                page_nums = int(total_nums / 10) + 1
            else:
                page_nums = int(total_nums / 10)

            for hit in response["hits"]["hits"]:
                hit_dict = {}
                try:
                    if "title" in hit["highlight"]:
                        hit_dict["title"] = "".join(hit["highlight"]["title"])
                    else:
                        hit_dict["title"] = hit["_source"]["title"]
                    if "job_desc" in hit["highlight"]:
                        hit_dict["content"] = "".join(hit["highlight"]["job_desc"])[:0]
                    else:
                        hit_dict["content"] = hit["_source"]["job_desc"][:0]

                    hit_dict["create_date"] = hit["_source"]["publish_time"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    # hit_dict["company_name"] = hit["_source"]["company_name"]
                    hit_dict["website"] = "拉勾网"
                    hit_list.append(hit_dict)
                except:
                    hit_dict["title"] = hit["_source"]["title"]
                    hit_dict["content"] = hit["_source"]["job_desc"]
                    hit_dict["create_date"] = hit["_source"]["publish_time"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    # hit_dict["company_name"] = hit["_source"]["company_name"]
                    hit_dict["source_site"] = "拉勾网"
                    hit_list.append(hit_dict)
        elif s_type == 'question':
            # 问答
            response = client.search(
                index="zhihu",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["topics", "title", "content"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "content": {}
                        }
                    }
                }
            )
            end_time = datetime.now()
            last_seconds = (end_time - start_time).total_seconds()
            total_nums = response["hits"]["total"]
            if (page % 10) > 0:
                page_nums = int(total_nums / 10) + 1
            else:
                page_nums = int(total_nums / 10)

            for hit in response["hits"]["hits"]:
                hit_dict = {}
                try:
                    if "title" in hit["highlight"]:
                        hit_dict["title"] = "".join(hit["highlight"]["title"])
                    elif "title" not in hit["_source"]:
                        hit_dict["title"] = "answer"
                    else:
                        hit_dict["title"] = hit["_source"]["title"]
                    if "content" in hit["highlight"]:
                        hit_dict["content"] = "".join(hit["highlight"]["content"])[:50]
                    else:
                        hit_dict["content"] = hit["_source"]["content"][:50]

                    hit_dict["create_date"] = hit["_source"]["create_time"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    hit_dict["website"] = "知乎"
                    hit_list.append(hit_dict)
                except:
                    if "title" in hit["_source"]:
                        hit_dict["title"] = hit["_source"]["title"]
                    else:
                        hit_dict["title"] = "answer"
                    hit_dict["content"] = hit["_source"]["content"]
                    hit_dict["create_date"] = hit["_source"]["update_time"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    hit_dict["source_site"] = "知乎"
                    hit_list.append(hit_dict)



        # docs总数
        jobbole_response = client.search(
            index="jobbole",
        )
        lagou_response = client.search(
            index="lagou",
        )
        zhihu_response = client.search(
            index="zhihu",
        )
        article_result = jobbole_response["hits"]["total"]
        position_result = lagou_response["hits"]["total"]
        question_result = zhihu_response["hits"]["total"]

        return render(request, "result.html", {"page": page,
                                               "s_type": s_type,
                                               "all_hits": hit_list,
                                               "key_words": key_words,
                                               "total_nums": total_nums,
                                               "page_nums": page_nums,
                                               "last_seconds": last_seconds,
                                               "topn_search": topn_search,
                                               "article_result": article_result,
                                               "position_result": position_result,
                                               "question_result": question_result,
                                               })
