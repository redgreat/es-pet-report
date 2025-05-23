from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Set
from fastapi.responses import PlainTextResponse
from datetime import datetime, timezone, timedelta
import redis
from redis.client import Pipeline
import json
import sys
import os
import configparser
import concurrent.futures
import threading
from functools import partial
import time

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 创建FastAPI应用
app = FastAPI(
    title="Redis Hash优化版时间差异分析API",
    description="使用线程池和分批处理优化的Redis Hash时间差异分析",
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

# 如果同时提供了用户名和密码，则拼接成 user:password 格式
if redis_user and redis_password:
    redis_password = f"{redis_user}:{redis_password}"

# 时间字段列表
TIME_FIELDS = ["mysqlInsertTime", "consumeTime", "receiveTime", "createTime"]

# 线程池大小
THREAD_POOL_SIZE = 10

# 每批处理的键数量
BATCH_SIZE = 1000

# 每个线程处理的批次大小
THREAD_BATCH_SIZE = 100

# 线程本地存储，用于每个线程维护自己的Redis连接
thread_local = threading.local()

class RedisRequest(BaseModel):
    """Redis请求模型"""
    pattern: Optional[str] = "*"  # 默认匹配所有键
    count: Optional[int] = 0  # 0表示不限制数量
    process_details: Optional[bool] = False  # 是否处理details字段中的JSON数据
    thread_count: Optional[int] = THREAD_POOL_SIZE  # 线程池大小

def get_redis_client():
    """获取线程本地的Redis客户端连接"""
    if not hasattr(thread_local, "redis"):
        thread_local.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,  # 自动将响应解码为字符串
            socket_timeout=10,  # 设置超时时间
            socket_connect_timeout=10,
            health_check_interval=30  # 定期检查连接健康状态
        )
    return thread_local.redis

def get_keys_batch(pattern: str, count: int = 0) -> List[List[str]]:
    """分批获取匹配模式的键

    Args:
        pattern: 键匹配模式
        count: 限制获取的键数量，0表示不限制

    Returns:
        List[List[str]]: 分批的键列表
    """
    r = get_redis_client()
    batches = []
    current_batch = []

    # 使用scan_iter迭代获取所有匹配的键
    cursor = '0'
    total_keys = 0

    while cursor != 0:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=BATCH_SIZE)

        for key in keys:
            current_batch.append(key)
            total_keys += 1

            # 当积累了一批键或者是最后一批时
            if len(current_batch) >= THREAD_BATCH_SIZE:
                batches.append(current_batch)
                current_batch = []

            # 如果达到了用户指定的数量限制，则退出循环
            if count > 0 and total_keys >= count:
                break

        if count > 0 and total_keys >= count:
            break

    # 添加最后一批剩余的键
    if current_batch:
        batches.append(current_batch)

    print(f"共获取 {total_keys} 个键，分为 {len(batches)} 批处理")
    return batches

def process_batch(keys: List[str], process_details: bool) -> Dict[str, Any]:
    """处理一批Redis HASH键

    Args:
        keys: 要处理的键列表
        process_details: 是否处理details字段中的JSON数据

    Returns:
        Dict: 包含处理结果的字典
    """
    r = get_redis_client()

    # 初始化结果
    max_diffs = {}
    total_diffs = {}
    diff_distributions = {}  # 用于记录时间差分布
    processed_docs = 0
    processed_details = 0  # 处理的details中的文档数
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

    # 使用管道批量获取值
    pipe = r.pipeline(transaction=False)
    for key in keys:
        pipe.hgetall(key)  # 获取整个hash
    hash_values = pipe.execute()

    # 处理每个hash
    for i, hash_data in enumerate(hash_values):
        if not hash_data:
            continue

        try:
            doc_id = keys[i]  # 使用Redis键作为文档ID
            processed_docs += 1

            # 处理hash中的时间字段
            time_info = {"earliest_test_time": earliest_test_time}
            process_hash_time_fields(hash_data, doc_id, max_diffs, total_diffs, diff_distributions, time_info)
            earliest_test_time = time_info["earliest_test_time"]

            # 处理details字段中的JSON数据
            if process_details and "details" in hash_data:
                details_count = process_details_field(hash_data["details"], doc_id, max_diffs, total_diffs, diff_distributions, time_info)
                earliest_test_time = time_info["earliest_test_time"]
                processed_details += details_count

        except Exception as e:
            print(f"处理Hash出错: {e}, key: {keys[i]}")

    return {
        "max_diffs": max_diffs,
        "total_diffs": total_diffs,
        "diff_distributions": diff_distributions,
        "processed_docs": processed_docs,
        "processed_details": processed_details,
        "earliest_test_time": earliest_test_time
    }

def process_hash_time_fields(hash_data: Dict, doc_id: str, max_diffs: Dict, total_diffs: Dict, diff_distributions: Dict, time_info: Dict) -> bool:
    """处理Hash中的时间字段

    Args:
        hash_data: Hash数据
        doc_id: 文档ID
        max_diffs: 最大时差字典
        total_diffs: 总时差字典
        diff_distributions: 时间差分布字典
        time_info: 包含earliest_test_time的字典

    Returns:
        bool: 是否成功处理
    """
    # 解析所有时间字段
    times = {}
    valid_doc = True

    for field in TIME_FIELDS:
        if field not in hash_data:
            valid_doc = False
            break

        try:
            # 解析时间字符串
            dt = datetime.strptime(hash_data[field], '%Y-%m-%d %H:%M:%S')
            times[field] = dt

            # 更新最早的mysqlInsertTime
            if field == "mysqlInsertTime":
                if time_info["earliest_test_time"] is None or dt < time_info["earliest_test_time"]:
                    time_info["earliest_test_time"] = dt
        except Exception as e:
            # print(f"解析时间字段 {field} 出错: {hash_data[field]}, 错误: {e}")
            valid_doc = False
            break

    if not valid_doc:
        return False

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

    return True

def process_details_field(details_str: str, parent_doc_id: str, max_diffs: Dict, total_diffs: Dict, diff_distributions: Dict, time_info: Dict) -> int:
    """处理details字段中的JSON数据

    Args:
        details_str: details字段的JSON字符串
        parent_doc_id: 父文档ID
        max_diffs: 最大时差字典
        total_diffs: 总时差字典
        diff_distributions: 时间差分布字典
        time_info: 包含earliest_test_time的字典

    Returns:
        int: 处理的details中的文档数
    """
    try:
        # 解析JSON数组
        details_array = json.loads(details_str)
        if not isinstance(details_array, list):
            # print(f"details字段不是数组: {details_str[:100]}...")
            return 0

        processed_count = 0

        # 处理每个details文档
        for i, detail in enumerate(details_array):
            if not isinstance(detail, dict):
                continue

            # 构造文档ID
            detail_id = f"{parent_doc_id}:detail:{i}"

            # 解析所有时间字段
            times = {}
            valid_doc = True

            for field in TIME_FIELDS:
                if field not in detail:
                    valid_doc = False
                    break

                try:
                    # 解析时间字符串
                    dt = datetime.strptime(detail[field], '%Y-%m-%d %H:%M:%S')
                    times[field] = dt

                    # 更新最早的mysqlInsertTime
                    if field == "mysqlInsertTime":
                        if time_info["earliest_test_time"] is None or dt < time_info["earliest_test_time"]:
                            time_info["earliest_test_time"] = dt
                except Exception as e:
                    # print(f"解析details时间字段 {field} 出错: {detail.get(field, 'N/A')}, 错误: {e}")
                    valid_doc = False
                    break

            if not valid_doc:
                continue

            processed_count += 1

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
                        "doc_id": detail_id
                    }

            # 计算首尾字段的总时差
            first_field = TIME_FIELDS[0]
            last_field = TIME_FIELDS[-1]
            total_pair_key = f"{first_field}_{last_field}"
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
                    "doc_id": detail_id
                }

        return processed_count
    except json.JSONDecodeError:
        # print(f"解析details JSON出错: {details_str[:100]}...")
        return 0
    except Exception as e:
        # print(f"处理details出错: {e}")
        return 0

def merge_results(results_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """合并多个线程的处理结果

    Args:
        results_list: 多个线程的处理结果列表

    Returns:
        Dict: 合并后的结果
    """
    if not results_list:
        return {}

    # 初始化合并结果
    merged_max_diffs = {}
    merged_total_diffs = {}
    merged_diff_distributions = {}
    total_processed_docs = 0
    total_processed_details = 0
    earliest_test_time = None

    # 初始化时差数据结构
    for i in range(len(TIME_FIELDS) - 1):
        field1 = TIME_FIELDS[i]
        field2 = TIME_FIELDS[i+1]
        pair_key = f"{field1}_{field2}"
        merged_max_diffs[pair_key] = {"diff": 0, "doc_id": None}
        merged_total_diffs[pair_key] = []
        merged_diff_distributions[pair_key] = {
            "under_0.5s": 0,
            "0.5s_to_1s": 0,
            "1s_to_2s": 0,
            "2s_to_5s": 0,
            "over_5s": 0
        }

    # 添加首尾字段的总时差
    total_pair_key = f"{TIME_FIELDS[0]}_{TIME_FIELDS[-1]}"
    merged_max_diffs[total_pair_key] = {"diff": 0, "doc_id": None}
    merged_total_diffs[total_pair_key] = []
    merged_diff_distributions[total_pair_key] = {
        "under_0.5s": 0,
        "0.5s_to_1s": 0,
        "1s_to_2s": 0,
        "2s_to_5s": 0,
        "over_5s": 0
    }

    # 合并每个线程的结果
    for result in results_list:
        if not result:
            continue

        # 合并处理文档数
        total_processed_docs += result.get("processed_docs", 0)
        total_processed_details += result.get("processed_details", 0)

        # 更新最早的测试时间
        result_earliest_time = result.get("earliest_test_time")
        if result_earliest_time and (earliest_test_time is None or result_earliest_time < earliest_test_time):
            earliest_test_time = result_earliest_time

        # 合并时差数据
        for pair_key in merged_total_diffs.keys():
            # 合并时差列表
            if pair_key in result.get("total_diffs", {}):
                merged_total_diffs[pair_key].extend(result["total_diffs"][pair_key])

            # 合并时差分布
            if pair_key in result.get("diff_distributions", {}):
                for dist_key in merged_diff_distributions[pair_key].keys():
                    merged_diff_distributions[pair_key][dist_key] += result["diff_distributions"][pair_key].get(dist_key, 0)

            # 更新最大时差
            if pair_key in result.get("max_diffs", {}):
                result_max_diff = result["max_diffs"][pair_key]
                if abs(result_max_diff["diff"]) > abs(merged_max_diffs[pair_key]["diff"]):
                    merged_max_diffs[pair_key] = result_max_diff

    # 计算平均时差
    avg_diffs = {}
    for pair_key, diffs in merged_total_diffs.items():
        if diffs:
            avg_diffs[pair_key] = sum(diffs) / len(diffs)
        else:
            avg_diffs[pair_key] = 0

    return {
        "max_diffs": merged_max_diffs,
        "avg_diffs": avg_diffs,
        "diff_distributions": merged_diff_distributions,
        "processed_docs": total_processed_docs,
        "processed_details": total_processed_details,
        "earliest_test_time": earliest_test_time
    }

def calculate_time_differences(pattern: str, count: int = 0, process_details: bool = True, thread_count: int = THREAD_POOL_SIZE) -> Dict[str, Any]:
    """使用线程池计算Redis中HASH格式数据的时间字段之间的时差

    Args:
        pattern: 键匹配模式
        count: 限制处理的键数量，0表示不限制
        process_details: 是否处理details字段中的JSON数据
        thread_count: 线程池大小

    Returns:
        Dict: 包含处理结果的字典
    """
    # 记录统计开始时间
    stats_start_time = datetime.now()
    query_start_time = datetime.now()

    # 分批获取键
    key_batches = get_keys_batch(pattern, count)

    # 如果没有匹配的键，返回空结果
    if not key_batches:
        return {
            "max_diffs": {},
            "avg_diffs": {},
            "diff_distributions": {},
            "processed_docs": 0,
            "processed_details": 0,
            "total_docs": 0,
            "query_start_time": query_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "earliest_test_time": "未找到",
            "stats_duration": 0
        }

    # 使用线程池处理每批键
    results = []

    # 调整线程池大小，不超过批次数量
    actual_thread_count = min(thread_count, len(key_batches))
    print(f"使用 {actual_thread_count} 个线程处理 {len(key_batches)} 批键")

    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_thread_count) as executor:
        # 提交任务到线程池
        future_to_batch = {
            executor.submit(process_batch, batch, process_details): i
            for i, batch in enumerate(key_batches)
        }

        # 获取结果
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            try:
                result = future.result()
                results.append(result)
                print(f"批次 {batch_index} 处理完成，处理文档 {result['processed_docs']}，details文档 {result['processed_details']}")
            except Exception as e:
                print(f"批次 {batch_index} 处理出错: {e}")

    # 合并所有线程的结果
    merged_results = merge_results(results)

    # 计算统计耗时
    stats_end_time = datetime.now()
    stats_duration = (stats_end_time - stats_start_time).total_seconds()

    # 格式化时间为字符串
    query_start_time_str = query_start_time.strftime('%Y-%m-%d %H:%M:%S')
    earliest_test_time_str = merged_results["earliest_test_time"].strftime('%Y-%m-%d %H:%M:%S') if merged_results.get("earliest_test_time") else "未找到"

    # 获取总键数
    total_keys = sum(len(batch) for batch in key_batches)

    # 添加额外信息
    merged_results.update({
        "total_docs": total_keys,
        "query_start_time": query_start_time_str,
        "earliest_test_time": earliest_test_time_str,
        "stats_duration": stats_duration
    })

    return merged_results

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
    # output.append(f"处理details文档数量: {results.get('processed_details', 0)}")
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
            total_processed = results["processed_docs"] + results.get("processed_details", 0)
            under_05s_percent = (under_05s / total_processed) * 100 if total_processed > 0 else 0

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
        total_processed = results["processed_docs"] + results.get("processed_details", 0)
        under_05s_percent = (under_05s / total_processed) * 100 if total_processed > 0 else 0

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
    return {"message": "Redis Hash优化版时间差异分析API服务正在运行"}

@app.get("/health")
async def health_check():
    """健康检查端点"""
    return {"status": "healthy"}

@app.post("/analyze")
async def analyze_time_diffs(request: RedisRequest):
    """分析时间差异"""
    try:
        # 计算时间差异
        results = calculate_time_differences(
            request.pattern,
            request.count,
            request.process_details,
            request.thread_count
        )

        # 返回结果
        return {
            "status": "success",
            "data": results,
            "formatted_results": format_results(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析时出错: {str(e)}")

@app.get("/text-report", response_class=PlainTextResponse)
async def get_text_report(
    pattern: Optional[str] = "*",
    count: Optional[int] = 0,
    process_details: Optional[bool] = True,
    thread_count: Optional[int] = THREAD_POOL_SIZE
):
    """获取文本格式的报告，count=0表示不限制文档数量"""
    try:
        # 计算时间差异
        results = calculate_time_differences(pattern, count, process_details, thread_count)

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
        count = 0
        cursor = '0'
        while cursor != 0:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=BATCH_SIZE)
            count += len(keys)
        return {"status": "success", "pattern": pattern, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取键数量时出错: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("redis_hash_optimized_api:app", host="0.0.0.0", port=8003, reload=True)
