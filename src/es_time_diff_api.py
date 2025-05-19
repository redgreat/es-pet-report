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
    size: Optional[int] = 1000000

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
    """计算时间字段之间的时差，使用分页查询获取所有文档"""
    # 东八区时区
    china_tz = timezone(timedelta(hours=8))
    utc_tz = timezone.utc

    # 构建查询，确保所有时间字段都存在
    must_clauses = [{"exists": {"field": field}} for field in TIME_FIELDS]

    # 获取文档总数
    total_docs = get_total_document_count(es, index)

    # 初始化结果
    max_diffs = {}
    total_diffs = {}
    processed_docs = 0

    # 初始化时差数据结构
    for i in range(len(TIME_FIELDS) - 1):
        field1 = TIME_FIELDS[i]
        field2 = TIME_FIELDS[i+1]
        pair_key = f"{field1}_{field2}"
        max_diffs[pair_key] = {"diff": 0, "doc_id": None}
        total_diffs[pair_key] = []

    # 添加首尾字段的总时差
    total_pair_key = f"{TIME_FIELDS[0]}_{TIME_FIELDS[-1]}"
    max_diffs[total_pair_key] = {"diff": 0, "doc_id": None}
    total_diffs[total_pair_key] = []

    # 设置每页大小，默认为1000
    page_size = min(size, 1000)  # 确保每页不超过1000条

    # 分页查询
    from_index = 0

    while from_index < total_docs:
        # 执行查询
        print(f"查询文档 {from_index} 到 {from_index + page_size}，总数 {total_docs}")
        response = es.search(
            index=index,
            size=page_size,
            from_=from_index,
            _source=TIME_FIELDS,
            query={
                "bool": {
                    "must": must_clauses
                }
            }
        )

        # 如果没有结果，退出循环
        hits = response['hits']['hits']
        if not hits:
            break

        # 处理每个文档
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

            # 更新总时差的最大值
            if abs(total_diff) > abs(max_diffs[total_pair_key]["diff"]):
                max_diffs[total_pair_key] = {
                    "diff": total_diff,
                    "doc_id": doc_id
                }

        # 更新已处理文档数
        processed_docs += len(hits)

        # 更新分页起始位置
        from_index += page_size

        # 如果已经处理的文档数达到了用户指定的大小限制，则退出循环
        if size != 0 and processed_docs >= size:
            break

    # 计算平均时差
    avg_diffs = {}
    for pair_key, diffs in total_diffs.items():
        if diffs:
            avg_diffs[pair_key] = sum(diffs) / len(diffs)
        else:
            avg_diffs[pair_key] = 0

    return {
        "max_diffs": max_diffs,
        "avg_diffs": avg_diffs,
        "processed_docs": processed_docs,
        "total_docs": total_docs
    }

def format_results(results: Dict[str, Any]) -> str:
    """格式化结果为可读文本"""
    output = []
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

            output.append(f"{field1} 到 {field2}，最大时差：{max_diff:.2f}秒，平均时差：{avg_diff:.2f}秒")

    # 输出总时差
    total_pair_key = f"{TIME_FIELDS[0]}_{TIME_FIELDS[-1]}"
    if total_pair_key in results["max_diffs"] and total_pair_key in results["avg_diffs"]:
        max_diff = results["max_diffs"][total_pair_key]["diff"]
        avg_diff = results["avg_diffs"][total_pair_key]

        output.append(f"\n{TIME_FIELDS[0]} 到 {TIME_FIELDS[-1]} 总最大时差：{max_diff:.2f}秒，总平均时差：{avg_diff:.2f}秒")

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
async def get_text_report(index_name: Optional[str] = None, size: Optional[int] = 100000):
    """获取文本格式的报告"""
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
async def get_text_report_ascii(index_name: Optional[str] = None, size: Optional[int] = 100000):
    """获取ASCII编码的文本格式报告（适用于PowerShell）"""
    try:
        # 获取ES客户端
        es = get_es_client()

        # 使用请求中的索引名称，如果未提供则使用默认值
        index_name = index_name or tar_index

        # 计算时间差异
        results = calculate_time_differences(es, index_name, size)

        # 格式化结果，但使用英文替代中文
        output = []
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

                output.append(f"{field1} to {field2}, Max diff: {max_diff:.2f} seconds, Avg diff: {avg_diff:.2f} seconds")

        # 输出总时差
        total_pair_key = f"{TIME_FIELDS[0]}_{TIME_FIELDS[-1]}"
        if total_pair_key in results["max_diffs"] and total_pair_key in results["avg_diffs"]:
            max_diff = results["max_diffs"][total_pair_key]["diff"]
            avg_diff = results["avg_diffs"][total_pair_key]

            output.append(f"\n{TIME_FIELDS[0]} to {TIME_FIELDS[-1]} Total max diff: {max_diff:.2f} seconds, Total avg diff: {avg_diff:.2f} seconds")

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
