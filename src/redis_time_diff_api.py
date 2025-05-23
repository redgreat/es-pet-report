from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from fastapi.responses import PlainTextResponse
from datetime import datetime, timezone, timedelta
import redis
import json
import sys
import os
import configparser

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 创建FastAPI应用
app = FastAPI(
    title="Redis时间差异分析API",
    description="分析Redis中时间字段之间的差异",
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

# Redis配置
redis_host = config.get("redis", "host")
redis_port = int(config.get("redis", "port"))
redis_db = int(config.get("redis", "db"))
redis_user = config.get("redis", "user") if config.has_option("redis", "user") else None
redis_password = config.get("redis", "password") if config.has_option("redis", "password") else None

# 时间字段列表 - 注意这里没有@timestamp
TIME_FIELDS = ["mysqlInsertTime", "consumeTime", "receiveTime", "createTime"]

class RedisRequest(BaseModel):
    """Redis请求模型"""
    pattern: Optional[str] = "*"  # 默认匹配所有键
    count: Optional[int] = 0  # 0表示不限制数量

def get_redis_client():
    """获取Redis客户端连接"""
    return redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        decode_responses=True  # 自动将响应解码为字符串
    )

def get_total_key_count(r: redis.Redis, pattern: str) -> int:
    """获取匹配模式的键总数"""
    try:
        # 使用scan_iter获取匹配的键数量
        count = 0
        for _ in r.scan_iter(match=pattern):
            count += 1
        return count
    except Exception as e:
        print(f"获取键总数时出错: {e}")
        return 0

def calculate_time_differences(r: redis.Redis, pattern: str, count: int = 0) -> Dict[str, Any]:
    """计算Redis中时间字段之间的时差"""
    # 记录统计开始时间
    stats_start_time = datetime.now()
    query_start_time = datetime.now()

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

    # 获取键总数
    total_keys = get_total_key_count(r, pattern)
    print(f"匹配模式 '{pattern}' 的键总数: {total_keys}")

    # 使用scan_iter迭代获取所有匹配的键
    batch_size = 1000  # 每批处理的键数量
    batch_count = 0
    batch_keys = []

    # 记录处理开始时间
    batch_start_time = datetime.now()

    # 遍历所有匹配的键
    for key in r.scan_iter(match=pattern):
        batch_keys.append(key)

        # 当积累了一批键或者是最后一批时，进行处理
        if len(batch_keys) >= batch_size:
            # 创建一个字典来存储和更新earliest_test_time
            time_info = {"earliest_test_time": earliest_test_time}
            process_batch(r, batch_keys, max_diffs, total_diffs, diff_distributions, time_info)
            earliest_test_time = time_info["earliest_test_time"]
            processed_docs += len(batch_keys)

            # 计算并显示处理耗时
            batch_time = (datetime.now() - batch_start_time).total_seconds()
            batch_count += 1
            print(f"批次 {batch_count}: 处理耗时 {batch_time:.2f}秒，键数量: {len(batch_keys)}，已处理: {processed_docs}，总数: {total_keys}")

            # 如果达到了用户指定的数量限制，则退出循环
            if count > 0 and processed_docs >= count:
                break

            # 重置批次和计时
            batch_keys = []
            batch_start_time = datetime.now()

    # 处理最后一批剩余的键
    if batch_keys:
        # 创建一个字典来存储和更新earliest_test_time
        time_info = {"earliest_test_time": earliest_test_time}
        process_batch(r, batch_keys, max_diffs, total_diffs, diff_distributions, time_info)
        earliest_test_time = time_info["earliest_test_time"]
        processed_docs += len(batch_keys)

        # 计算并显示处理耗时
        batch_time = (datetime.now() - batch_start_time).total_seconds()
        batch_count += 1
        print(f"批次 {batch_count}: 处理耗时 {batch_time:.2f}秒，键数量: {len(batch_keys)}，已处理: {processed_docs}，总数: {total_keys}")

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
        "diff_distributions": diff_distributions,
        "processed_docs": processed_docs,
        "total_docs": total_keys,
        "query_start_time": query_start_time_str,
        "earliest_test_time": earliest_test_time_str,
        "stats_duration": stats_duration
    }

def process_batch(r: redis.Redis, keys: List[str], max_diffs: Dict, total_diffs: Dict, diff_distributions: Dict, time_info: Dict):
    """处理一批Redis键

    Args:
        r: Redis客户端
        keys: 要处理的键列表
        max_diffs: 最大时差字典
        total_diffs: 总时差字典
        diff_distributions: 时间差分布字典
        time_info: 包含earliest_test_time的字典，用于传递和更新最早时间
    """
    # 使用管道批量获取值
    pipe = r.pipeline()
    for key in keys:
        pipe.get(key)
    values = pipe.execute()

    # 处理每个值
    for i, value in enumerate(values):
        if not value:
            continue

        try:
            # 解析JSON数据
            doc = json.loads(value)
            doc_id = keys[i]  # 使用Redis键作为文档ID

            # 解析所有时间字段
            times = {}
            valid_doc = True

            for field in TIME_FIELDS:
                # 处理字段名称的大小写差异
                field_key = field
                if field.lower() == "mysqlinserttime" and "MySQLInsertTime" in doc:
                    field_key = "MySQLInsertTime"
                elif field not in doc and field.lower() in doc:
                    field_key = field.lower()
                elif field not in doc and field.upper() in doc:
                    field_key = field.upper()

                if field_key not in doc:
                    valid_doc = False
                    break

                try:
                    # 解析时间字符串
                    dt = datetime.strptime(doc[field_key], '%Y-%m-%d %H:%M:%S')
                    times[field] = dt

                    # 更新最早的mysqlInsertTime
                    if field == "mysqlInsertTime":
                        if time_info["earliest_test_time"] is None or dt < time_info["earliest_test_time"]:
                            time_info["earliest_test_time"] = dt
                except Exception as e:
                    print(f"解析时间字段 {field_key} 出错: {doc[field_key]}, 错误: {e}")
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
            total_pair_key = f"{first_field}_{last_field}"  # 定义总时差的键
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
        except json.JSONDecodeError:
            print(f"解析JSON出错: {value}")
        except Exception as e:
            print(f"处理文档出错: {e}")

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
    return {"message": "Redis时间差异分析API服务正在运行"}

@app.get("/health")
async def health_check():
    """健康检查端点"""
    return {"status": "healthy"}

@app.post("/analyze")
async def analyze_time_diffs(request: RedisRequest):
    """分析时间差异"""
    try:
        # 获取Redis客户端
        r = get_redis_client()

        # 计算时间差异
        results = calculate_time_differences(r, request.pattern, request.count)

        # 返回结果
        return {
            "status": "success",
            "data": results,
            "formatted_results": format_results(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析时出错: {str(e)}")

@app.get("/text-report", response_class=PlainTextResponse)
async def get_text_report(pattern: Optional[str] = "*", count: Optional[int] = 0):
    """获取文本格式的报告，count=0表示不限制文档数量"""
    try:
        # 获取Redis客户端
        r = get_redis_client()

        # 计算时间差异
        results = calculate_time_differences(r, pattern, count)

        # 返回格式化的文本结果，使用PlainTextResponse确保正确显示换行
        text_result = format_results(results)

        # 创建响应并设置正确的编码
        response = PlainTextResponse(content=text_result)
        response.headers["Content-Type"] = "text/plain; charset=utf-8"
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成报告时出错: {str(e)}")

@app.get("/count")
async def get_count(pattern: Optional[str] = "*"):
    """获取匹配模式的键数量"""
    try:
        r = get_redis_client()
        count = get_total_key_count(r, pattern)
        return {"status": "success", "pattern": pattern, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取键数量时出错: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("redis_time_diff_api:app", host="0.0.0.0", port=8001, reload=True)
