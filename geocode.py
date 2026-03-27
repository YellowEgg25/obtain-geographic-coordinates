# coding:utf-8
import os
import json
import time
from pathlib import Path
from coordinate_transformation.CoordinatesConverter import bd09lltowgs84

import requests
import pandas as pd

# ================== 参数区 ==================
BASE_DIR = Path(".")
INPUT_FILE = Path("data/address.xlsx")
OUT_DIR = Path("chunks")
CHECKPOINT_FILE = Path("checkpoint.json")
FINAL_FILE = Path("final_result.xlsx")

AK = os.getenv("BAIDU_AK")

CHUNK_SIZE = 4940          # 每天处理 5000 条
RETRY_TIMES = 3            # 单条地址失败后的重试次数
SLEEP_BETWEEN = 0.15       # 每次请求间隔，避免过快

OUT_DIR.mkdir(parents=True, exist_ok=True)


# ================== 地理编码函数 ==================
def get_location(address, ak):
    """
    返回: lng, lat, status
    成功时 lng/lat 为数值，status=0
    失败时 lng/lat 为 None
    """
    url = "https://api.map.baidu.com/geocoding/v3/"
    params = {
        "address": address,
        "output": "json",
        "ak": ak
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        if data.get("status") != 0:
            return None, None, data.get("status")

        loc = data["result"]["location"]
        lng, lat = bd09lltowgs84(loc["lng"], loc["lat"])  # 坐标转换
        return lng, lat, 0

    except Exception:
        return None, None, -1


# ================== 断点信息 ==================
def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"next_row": 0}


def save_checkpoint(next_row):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"next_row": next_row}, f, ensure_ascii=False, indent=2)


# ================== 合并所有分块 ==================
def merge_chunks():
    chunk_files = sorted(OUT_DIR.glob("chunk_*.xlsx"))
    if not chunk_files:
        print("没有找到可合并的分块文件。")
        return

    dfs = []
    for file in chunk_files:
        dfs.append(pd.read_excel(file))

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_excel(FINAL_FILE, index=False)
    print(f"最终结果已保存：{FINAL_FILE}")


# ================== 主流程 ==================
def main():
    df = pd.read_excel(INPUT_FILE)

    # 确保索引连续，方便断点记录
    df = df.reset_index(drop=True)

    ck = load_checkpoint()
    start = ck["next_row"]

    if start >= len(df):
        print("全部数据已经处理完毕，开始合并结果。")
        merge_chunks()
        return

    end = min(start + CHUNK_SIZE, len(df))
    print(f"本次处理行号：{start} 到 {end - 1}")

    df_chunk = df.iloc[start:end].copy()
    df_chunk["X"] = ""
    df_chunk["Y"] = ""

    for i in range(len(df_chunk)):
        place_name = str(df_chunk.loc[df_chunk.index[i], "注册地址"]).strip()

        lng = None
        lat = None
        status = None

        for attempt in range(RETRY_TIMES + 1):
            lng, lat, status = get_location(place_name, AK)
            if lng is not None and lat is not None:
                break
            if attempt < RETRY_TIMES:
                time.sleep(2)

        if lng is None or lat is None:
            df_chunk.loc[df_chunk.index[i], "X"] = ""
            df_chunk.loc[df_chunk.index[i], "Y"] = ""
            print(f"第 {start + i} 行失败：{place_name}")
        else:
            df_chunk.loc[df_chunk.index[i], "X"] = lng
            df_chunk.loc[df_chunk.index[i], "Y"] = lat
            print(f"第 {start + i} 行：{place_name} -> {lng}, {lat}")

        time.sleep(SLEEP_BETWEEN)

    # 保存本次分块结果
    chunk_no = start // CHUNK_SIZE + 1
    chunk_file = OUT_DIR / f"chunk_{chunk_no:03d}_{start+1:05d}_{end:05d}.xlsx"
    df_chunk.rename(columns={"X": "X", "Y": "Y"}, inplace=True)
    df_chunk.to_excel(chunk_file, index=False)
    print(f"本次分块已保存：{chunk_file}")

    # 更新断点
    save_checkpoint(end)
    print(f"checkpoint 已更新到第 {end} 行")

    # 如果全部完成，则自动合并
    if end >= len(df):
        print("全部完成，开始合并。")
        merge_chunks()


if __name__ == "__main__":
    main()
