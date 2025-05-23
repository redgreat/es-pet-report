from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel
from typing import Optional, Dict, Any
from fastapi.responses import PlainTextResponse
from datetime import datetime, timezone, timedelta
from elasticsearch import Elasticsearch
import sys
import os
import configparser

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 创建FastAPI应用
app = FastAPI(
    title="ES时间差异分析API",
    description="分析Elasticsearch中时间字段之间的差异",
    version="1.0.0"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有方法
    allow_headers=["*"],  # 允许所有头
)

# 数据库连接定义
config = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
config_path = os.path.join(project_root, "conf", "db.cnf")
config.read(config_path)

# 目标ElasticSearch配置
tar_host = config.get("target_pet", "host")
tar_port = int(config.get("target_pet", "port"))
tar_user = config.get("target_pet", "user")
tar_password = config.get("target_pet", "password")
tar_index = config.get("target_pet", "index_name")

# ElasticSearch连接配置
ES_SETTINGS = {
    "hosts": [f"http://{tar_host}:{tar_port}"],
    "http_auth": (tar_user, tar_password) if tar_user and tar_password else None,
    "timeout": 60
}

# 时间字段列表
TIME_FIELDS = ["mysqlInsertTime", "consumeTime", "receiveTime", "createTime", "@timestamp"]

class IndexRequest(BaseModel):
    """索引请求模型"""
    index_name: Optional[str] = None
    size: Optional[int] = 0  # 0表示不限制文档数量

def get_es_client():
    """获取ES客户端连接"""
    return Elasticsearch(**ES_SETTINGS)

def get_total_document_count(es: Elasticsearch, index: str) -> int:
    """获取索引中的文档总数"""
    try:
        count_response = es.count(index=index)
        return count_response['count']
    except Exception as e:
        print(f"获取文档总数时出错: {e}")
        return 0

def calculate_time_differences(es: Elasticsearch, index: str, size: int = 1000) -> Dict[str, Any]:
    """计算时间字段之间的时差，使用Scroll API获取所有文档"""
    # 东八区时区
    china_tz = timezone(timedelta(hours=8))
    utc_tz = timezone.utc

    # 记录统计开始时间
    query_start_time = datetime.now(china_tz)
    stats_start_time = datetime.now()

    # 构建查询，确保所有时间字段都存在
    must_clauses = [{"exists": {"field": field}} for field in TIME_FIELDS]

    # 获取文档总数
    total_docs = get_total_document_count(es, index)

    # 初始化结果
    max_diffs = {}
    total_diffs = {}
    diff_distributions = {}  # 用于记录时间差分布
    processed_docs = 0
    earliest_test_time = None  # 用于记录最早的mysqlInsertTime

    # 初始化时差数据结构
    for i in range(len(TIME_FIELDS) - 1):
        field1 = TIME_FIELDS[i]
        field2 = TIME_FIELDS[i+1]
        pair_key = f"{field1}_{field2}"
        max_diffs[pair_key] = {"diff": 0, "doc_id": None}
        total_diffs[pair_key] = []
        # 初始化时间差分布统计
        diff_distributions[pair_key] = {
            "under_0.5s": 0,  # 0.5秒以下
            "0.5s_to_1s": 0,  # 0.5-1秒
            "1s_to_2s": 0,    # 1-2秒
            "2s_to_5s": 0,    # 2-5秒
            "over_5s": 0      # 5秒以上
        }

    # 添加首尾字段的总时差
    total_pair_key = f"{TIME_FIELDS[0]}_{TIME_FIELDS[-1]}"
    max_diffs[total_pair_key] = {"diff": 0, "doc_id": None}
    total_diffs[total_pair_key] = []
    # 初始化总时间差分布统计
    diff_distributions[total_pair_key] = {
        "under_0.5s": 0,
        "0.5s_to_1s": 0,
        "1s_to_2s": 0,
        "2s_to_5s": 0,
        "over_5s": 0
    }

    # 设置每批大小，默认为5000
    batch_size = min(size, 5000) if size > 0 else 5000

    # 初始化Scroll查询
    print(f"开始Scroll查询，批次大小: {batch_size}，总文档数: {total_docs}")

    # 记录查询开始时间，用于性能监控
    batch_start_time = datetime.now()

    response = es.search(
        index=index,
        scroll='5m',  # 设置滚动时间窗口为5分钟，减少超时风险
        size=batch_size,
        _source=TIME_FIELDS,  # 只获取需要的字段
        query={
            "bool": {
                "must": must_clauses
            }
        },
        sort=["_doc"],  # 按文档ID排序，这通常是最快的排序方式
        request_timeout=60  # 设置请求超时时间
    )

    # 获取第一批结果和scroll_id
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    scroll_size = len(hits)

    # 处理所有批次的文档
    batch_count = 1
    while scroll_size > 0:
        # 计算并显示查询耗时
        batch_time = (datetime.now() - batch_start_time).total_seconds()
        print(f"批次 {batch_count}: 查询耗时 {batch_time:.2f}秒，文档数: {scroll_size}，已处理: {processed_docs}，总数: {total_docs}")
        batch_start_time = datetime.now()
        batch_count += 1

        # 处理当前批次的每个文档
        for hit in hits:
            doc = hit['_source']
            doc_id = hit['_id']

            # 解析所有时间字段
            times = {}
            valid_doc = True

            for field in TIME_FIELDS:
                if field not in doc:
                    valid_doc = False
                    break

                try:
                    if field == '@timestamp':
                        # 处理ISO格式的UTC时间
                        dt_str = doc[field]
                        if 'Z' in dt_str:
                            dt_str = dt_str.replace('Z', '+00:00')
                        dt = datetime.fromisoformat(dt_str)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=utc_tz)
                    else:
                        # 处理标准格式的东八区时间
                        dt = datetime.strptime(doc[field], '%Y-%m-%d %H:%M:%S')
                        dt = dt.replace(tzinfo=china_tz)

                    times[field] = dt

                    # 更新最早的mysqlInsertTime
                    if field == "mysqlInsertTime":
                        if earliest_test_time is None or dt < earliest_test_time:
                            earliest_test_time = dt
                except Exception as e:
                    print(f"解析时间字段 {field} 出错: {doc[field]}, 错误: {e}")
                    valid_doc = False
                    break

            if not valid_doc:
                continue

            # 计算相邻字段的时差
            for i in range(len(TIME_FIELDS) - 1):
                field1 = TIME_FIELDS[i]
                field2 = TIME_FIELDS[i+1]
                pair_key = f"{field1}_{field2}"

                diff_seconds = (times[field2] - times[field1]).total_seconds()
                total_diffs[pair_key].append(diff_seconds)

                # 更新时间差分布统计
                abs_diff = abs(diff_seconds)
                if abs_diff < 0.5:
                    diff_distributions[pair_key]["under_0.5s"] += 1
                elif abs_diff < 1.0:
                    diff_distributions[pair_key]["0.5s_to_1s"] += 1
                elif abs_diff < 2.0:
                    diff_distributions[pair_key]["1s_to_2s"] += 1
                elif abs_diff < 5.0:
                    diff_distributions[pair_key]["2s_to_5s"] += 1
                else:
                    diff_distributions[pair_key]["over_5s"] += 1

                # 更新最大时差（使用绝对值比较，但保存原始值）
                if abs(diff_seconds) > abs(max_diffs[pair_key]["diff"]):
                    max_diffs[pair_key] = {
                        "diff": diff_seconds,
                        "doc_id": doc_id
                    }

            # 计算首尾字段的总时差
            first_field = TIME_FIELDS[0]
            last_field = TIME_FIELDS[-1]
            total_diff = (times[last_field] - times[first_field]).total_seconds()
            total_diffs[total_pair_key].append(total_diff)

            # 更新总时差分布统计
            abs_total_diff = abs(total_diff)
            if abs_total_diff < 0.5:
                diff_distributions[total_pair_key]["under_0.5s"] += 1
            elif abs_total_diff < 1.0:
                diff_distributions[total_pair_key]["0.5s_to_1s"] += 1
            elif abs_total_diff < 2.0:
                diff_distributions[total_pair_key]["1s_to_2s"] += 1
            elif abs_total_diff < 5.0:
                diff_distributions[total_pair_key]["2s_to_5s"] += 1
            else:
                diff_distributions[total_pair_key]["over_5s"] += 1

            # 更新总时差的最大值
            if abs(total_diff) > abs(max_diffs[total_pair_key]["diff"]):
                max_diffs[total_pair_key] = {
                    "diff": total_diff,
                    "doc_id": doc_id
                }

        # 更新已处理文档数
        processed_docs += scroll_size

        # 如果已经处理的文档数达到了用户指定的大小限制，则退出循环
        if size > 0 and processed_docs >= size:
            break

        # 记录处理耗时
        process_time = (datetime.now() - batch_start_time).total_seconds()
        print(f"批次 {batch_count-1}: 处理耗时 {process_time:.2f}秒")

        # 记录下一批查询开始时间
        batch_start_time = datetime.now()

        # 获取下一批结果
        response = es.scroll(scroll_id=scroll_id, scroll='5m', request_timeout=60)
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        scroll_size = len(hits)

    # 清理scroll
    try:
        es.clear_scroll(scroll_id=scroll_id)
        print("Scroll查询已清理")
    except Exception as e:
        print(f"清理Scroll查询时出错: {e}")

    # 计算平均时差
    avg_diffs = {}
    for pair_key, diffs in total_diffs.items():
        if diffs:
            avg_diffs[pair_key] = sum(diffs) / len(diffs)
        else:
            avg_diffs[pair_key] = 0

    # 计算统计耗时
    stats_end_time = datetime.now()
    stats_duration = (stats_end_time - stats_start_time).total_seconds()

    # 格式化时间为字符串
    query_start_time_str = query_start_time.strftime('%Y-%m-%d %H:%M:%S')
    earliest_test_time_str = earliest_test_time.strftime('%Y-%m-%d %H:%M:%S') if earliest_test_time else "未找到"

    return {
        "max_diffs": max_diffs,
        "avg_diffs": avg_diffs,
        "diff_distributions": diff_distributions,  # 时间差分布统计
        "processed_docs": processed_docs,
        "total_docs": total_docs,
        "query_start_time": query_start_time_str,
        "earliest_test_time": earliest_test_time_str,
        "stats_duration": stats_duration  # 统计耗时（秒）
    }

def format_results(results: Dict[str, Any]) -> str:
    """格式化结果为可读文本"""
    output = []
    # 添加压测开始时间和统计完成时间
    output.append(f"本次压测开始时间: {results.get('earliest_test_time', '未找到')}")
    output.append(f"统计完成时间: {results.get('query_start_time', '未记录')}")
    output.append(f"统计耗时: {results.get('stats_duration', 0):.2f}秒")
    output.append("")
    output.append(f"总文档数量: {results['total_docs']}")
    output.append(f"处理文档数量: {results['processed_docs']}")
    output.append("\n相邻时间字段之间的时差:")

    # 输出每对相邻字段的时差
    for i in range(len(TIME_FIELDS) - 1):
        field1 = TIME_FIELDS[i]
        field2 = TIME_FIELDS[i+1]
        pair_key = f"{field1}_{field2}"

        if pair_key in results["max_diffs"] and pair_key in results["avg_diffs"]:
            max_diff = results["max_diffs"][pair_key]["diff"]
            avg_diff = results["avg_diffs"][pair_key]

            # 获取时间差分布
            dist = results["diff_distributions"].get(pair_key, {})
            under_05s = dist.get("under_0.5s", 0)
            under_05s_percent = (under_05s / results["processed_docs"]) * 100 if results["processed_docs"] > 0 else 0

            output.append(f"{field1} 到 {field2}，最大时差：{max_diff:.2f}秒，平均时差：{avg_diff:.2f}秒")
            output.append(f"  时间差分布：")
            output.append(f"    0.5秒以下: {under_05s}个文档 ({under_05s_percent:.2f}%)")
            output.append(f"    0.5-1秒: {dist.get('0.5s_to_1s', 0)}个文档")
            output.append(f"    1-2秒: {dist.get('1s_to_2s', 0)}个文档")
            output.append(f"    2-5秒: {dist.get('2s_to_5s', 0)}个文档")
            output.append(f"    5秒以上: {dist.get('over_5s', 0)}个文档")

    # 输出总时差
    total_pair_key = f"{TIME_FIELDS[0]}_{TIME_FIELDS[-1]}"
    if total_pair_key in results["max_diffs"] and total_pair_key in results["avg_diffs"]:
        max_diff = results["max_diffs"][total_pair_key]["diff"]
        avg_diff = results["avg_diffs"][total_pair_key]

        # 获取总时差分布
        dist = results["diff_distributions"].get(total_pair_key, {})
        under_05s = dist.get("under_0.5s", 0)
        under_05s_percent = (under_05s / results["processed_docs"]) * 100 if results["processed_docs"] > 0 else 0

        output.append(f"\n{TIME_FIELDS[0]} 到 {TIME_FIELDS[-1]} 总最大时差：{max_diff:.2f}秒，总平均时差：{avg_diff:.2f}秒")
        output.append(f"  总时差分布：")
        output.append(f"    0.5秒以下: {under_05s}个文档 ({under_05s_percent:.2f}%)")
        output.append(f"    0.5-1秒: {dist.get('0.5s_to_1s', 0)}个文档")
        output.append(f"    1-2秒: {dist.get('1s_to_2s', 0)}个文档")
        output.append(f"    2-5秒: {dist.get('2s_to_5s', 0)}个文档")
        output.append(f"    5秒以上: {dist.get('over_5s', 0)}个文档")

    return "\n".join(output)

@app.get("/")
async def root():
    """API根路径"""
    return {"message": "ES时间差异分析API服务正在运行"}

@app.get("/health")
async def health_check():
    """健康检查端点"""
    return {"status": "healthy"}

@app.post("/analyze")
async def analyze_time_diffs(request: IndexRequest):
    """分析时间差异"""
    try:
        # 获取ES客户端
        es = get_es_client()

        # 使用请求中的索引名称，如果未提供则使用默认值
        index_name = request.index_name or tar_index

        # 计算时间差异
        results = calculate_time_differences(es, index_name, request.size)

        # 返回结果
        return {
            "status": "success",
            "data": results,
            "formatted_results": format_results(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析时出错: {str(e)}")

@app.get("/text-report", response_class=PlainTextResponse)
async def get_text_report(index_name: Optional[str] = None, size: Optional[int] = 0):
    """获取文本格式的报告，size=0表示不限制文档数量"""
    try:
        # 获取ES客户端
        es = get_es_client()

        # 使用请求中的索引名称，如果未提供则使用默认值
        index_name = index_name or tar_index

        # 计算时间差异
        results = calculate_time_differences(es, index_name, size)

        # 返回格式化的文本结果，使用PlainTextResponse确保正确显示换行
        text_result = format_results(results)

        # 创建响应并设置正确的编码
        response = PlainTextResponse(content=text_result)
        response.headers["Content-Type"] = "text/plain; charset=utf-8"
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成报告时出错: {str(e)}")

@app.get("/text-report-ascii", response_class=PlainTextResponse)
async def get_text_report_ascii(index_name: Optional[str] = None, size: Optional[int] = 0):
    """获取ASCII编码的文本格式报告（适用于PowerShell），size=0表示不限制文档数量"""
    try:
        # 获取ES客户端
        es = get_es_client()

        # 使用请求中的索引名称，如果未提供则使用默认值
        index_name = index_name or tar_index

        # 计算时间差异
        results = calculate_time_differences(es, index_name, size)

        # 格式化结果，但使用英文替代中文
        output = []
        # 添加压测开始时间和统计完成时间
        output.append(f"Test start time: {results.get('earliest_test_time', 'Not found')}")
        output.append(f"Statistics completion time: {results.get('query_start_time', 'Not recorded')}")
        output.append(f"Statistics duration: {results.get('stats_duration', 0):.2f} seconds")
        output.append("")
        output.append(f"Total documents: {results['total_docs']}")
        output.append(f"Processed documents: {results['processed_docs']}")
        output.append("\nTime differences between adjacent time fields:")

        # 输出每对相邻字段的时差
        for i in range(len(TIME_FIELDS) - 1):
            field1 = TIME_FIELDS[i]
            field2 = TIME_FIELDS[i+1]
            pair_key = f"{field1}_{field2}"

            if pair_key in results["max_diffs"] and pair_key in results["avg_diffs"]:
                max_diff = results["max_diffs"][pair_key]["diff"]
                avg_diff = results["avg_diffs"][pair_key]

                # 获取时间差分布
                dist = results["diff_distributions"].get(pair_key, {})
                under_05s = dist.get("under_0.5s", 0)
                under_05s_percent = (under_05s / results["processed_docs"]) * 100 if results["processed_docs"] > 0 else 0

                output.append(f"{field1} to {field2}, Max diff: {max_diff:.2f} seconds, Avg diff: {avg_diff:.2f} seconds")
                output.append(f"  Time diff distribution:")
                output.append(f"    Under 0.5s: {under_05s} docs ({under_05s_percent:.2f}%)")
                output.append(f"    0.5s-1s: {dist.get('0.5s_to_1s', 0)} docs")
                output.append(f"    1s-2s: {dist.get('1s_to_2s', 0)} docs")
                output.append(f"    2s-5s: {dist.get('2s_to_5s', 0)} docs")
                output.append(f"    Over 5s: {dist.get('over_5s', 0)} docs")

        # 输出总时差
        total_pair_key = f"{TIME_FIELDS[0]}_{TIME_FIELDS[-1]}"
        if total_pair_key in results["max_diffs"] and total_pair_key in results["avg_diffs"]:
            max_diff = results["max_diffs"][total_pair_key]["diff"]
            avg_diff = results["avg_diffs"][total_pair_key]

            # 获取总时差分布
            dist = results["diff_distributions"].get(total_pair_key, {})
            under_05s = dist.get("under_0.5s", 0)
            under_05s_percent = (under_05s / results["processed_docs"]) * 100 if results["processed_docs"] > 0 else 0

            output.append(f"\n{TIME_FIELDS[0]} to {TIME_FIELDS[-1]} Total max diff: {max_diff:.2f} seconds, Total avg diff: {avg_diff:.2f} seconds")
            output.append(f"  Total time diff distribution:")
            output.append(f"    Under 0.5s: {under_05s} docs ({under_05s_percent:.2f}%)")
            output.append(f"    0.5s-1s: {dist.get('0.5s_to_1s', 0)} docs")
            output.append(f"    1s-2s: {dist.get('1s_to_2s', 0)} docs")
            output.append(f"    2s-5s: {dist.get('2s_to_5s', 0)} docs")
            output.append(f"    Over 5s: {dist.get('over_5s', 0)} docs")

        # 创建响应并设置ASCII编码
        text_result = "\n".join(output)
        response = PlainTextResponse(content=text_result)
        response.headers["Content-Type"] = "text/plain; charset=ascii"
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")

@app.get("/count")
async def get_count(index_name: Optional[str] = None):
    """获取索引中的文档数量"""
    try:
        es = get_es_client()
        index_name = index_name or tar_index
        count = get_total_document_count(es, index_name)
        return {"status": "success", "index": index_name, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文档数量时出错: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("es_time_diff_api:app", host="0.0.0.0", port=8000, reload=True)
