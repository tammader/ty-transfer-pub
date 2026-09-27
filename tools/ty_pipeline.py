# -*- coding: utf-8 -*-
"""云盘 -> 分卷 -> 中转远端/in2 流水线 (GitHub Actions 上运行)

流程:
  1. 比对: 收集 目标网盘/out2 + 副网盘/out2 + 中转远端(in2/out/lada_videos)
     的已有编号, 云盘 里命中的直接标记"已下载_"跳过, 不重复下载
  2. 取直链, ffmpeg 直接从直链切分(不落源文件, 省一半磁盘)
  3. 逐段上传 中转远端/in2 -> 云端加 _D 标记 -> 立即删本地该段
  4. 全部上传完 -> 源文件改名"已下载_" -> 处理下一个

环境变量(secrets): TY_AUTH, TY_CLOUD_ID, RCLONE_CONF
用法: python ty_pipeline.py [--max-files 2] [--max-file-gb 8] [--max-total-gb 10]
"""
import argparse
import glob
import io
import json
import os
import re
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ty_cloud import TyCloud, parse_account

import pathcfg          # 路径真值来自配置: CI=Secret PATHS_JSON, 本机=paths.local.json
WORK = "/tmp/tywork"
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".wmv")
MAX_PART = 550 * 1024 ** 2          # 分卷目标, 保证 <600MB
MAX_SEG_BYTES = 700 * 1024 ** 2     # 单段硬上限(关键帧稀疏时可能超出目标)
SEG_TARGET = 450 * 1024 ** 2        # 每段目标大小, 留出余量保证不超硬上限
DONE_PREFIX = "已下载_"
IN_NAME = "in"
OD_DEST = pathcfg.require("TT_OD_DEST")


def _find_conf():
    """rclone 配置路径: 支持环境变量, 自动探测 Linux/Windows"""
    for c in (os.environ.get("RCLONE_CONF_PATH", ""),
              os.path.expanduser("~/.config/rclone/rclone.conf"),
              os.path.join(os.environ.get("APPDATA", ""), "rclone", "rclone.conf"),
              os.path.expanduser("~/AppData/Roaming/rclone/rclone.conf")):
        if c and os.path.exists(c):
            return c
    return os.path.expanduser("~/.config/rclone/rclone.conf")


RCLONE_CONF = _find_conf()

# 比对源: 可直接 rclone 读的(中转远端 在 runner 上可达)
COMPARE_REMOTES = [x for x in pathcfg.require("TT_COMPARE_REMOTES").split(",") if x]
# 目标网盘/out2 + 副网盘/out2 挂在本地 alist, runner 不可达 ->
# 由本机 export_compare.py 导出编号清单随仓库带上, runner 读它比对
COMPARE_OD_MANIFEST = pathcfg.require("TT_OD_MANIFEST")   # 优先从这里拉清单
FAILED_MANIFEST = pathcfg.require("TT_FAILED_MANIFEST")
DONE_MANIFEST = pathcfg.require("TT_DONE_MANIFEST")   # 已完成编号兜底(rename 失败时记, 比对时纳入)
GD_MANIFEST = pathcfg.require("TT_GD_MANIFEST")   # 源远端/out 的编号(由 out_sync 生成)
FAIL_MAX = 2                                            # 连续失败多少次后永久跳过
# 清单单一来源: 中转远端:报告区 (由本机 export_compare.py 生成上传)
# 本机运行时 alist 可达, 这两个也一起实时读
COMPARE_REMOTES_LOCAL = [x for x in pathcfg.require("TT_COMPARE_REMOTES_LOCAL").split(",") if x]


def sh(cmd, timeout=None):
    return subprocess.run(cmd, capture_output=True, text=True,
                          errors="replace", timeout=timeout)


def rclone(args, timeout=None):
    return subprocess.run(["rclone"] + args + ["--config", RCLONE_CONF],
                          capture_output=True, text=True, errors="replace",
                          timeout=timeout)


def human(b):
    return "%.0f MB" % (b / 1048576) if b < 1024 ** 3 else "%.2f GB" % (b / 1024 ** 3)


def probe_dur(ffprobe, path):
    """ffprobe 取容器时长(秒); 拿不到返回 0.0"""
    try:
        out = sh([ffprobe, "-v", "error", "-show_entries", "format=duration",
                  "-of", "csv=p=0", path], timeout=300).stdout or ""
    except Exception:
        return 0.0
    for ln in out.splitlines():
        try:
            v = float(ln.strip())
            if v > 0:
                return v
        except ValueError:
            pass
    return 0.0


def probe_dur_packets(ffprobe, path):
    """容器缺 index/无 duration 时: 顺序解包, 取最后一个视频包的 pts_time 当时长。
    顺序读不依赖 seek/index, 对 AVI 这类无索引文件有效; 本地文件上跑, 代价可接受。"""
    try:
        out = sh([ffprobe, "-v", "error", "-select_streams", "v:0",
                  "-show_entries", "packet=pts_time",
                  "-of", "csv=p=0", path], timeout=3600).stdout or ""
    except Exception:
        return 0.0
    vals = []
    for tok in out.replace(",", "\n").split():
        try:
            vals.append(float(tok))
        except ValueError:
            pass
    return max(vals) if vals else 0.0


def stem(name):
    """复刻本地 worker: 去扩展名/_D/前缀/.partNNN, 小写"""
    s = os.path.splitext(name)[0]
    s = re.sub(r"_D$", "", s)
    s = re.sub(r"^(已处理_|D_)", "", s)
    s = re.sub(r"\.part\d+$", "", s)
    return s.strip().lower()


def load_failed():
    """拉取失败清单 -> {stem: 次数}"""
    tmp = "/tmp/failed_from_od.txt"
    out = {}
    try:
        r = rclone(["copyto", FAILED_MANIFEST, tmp], timeout=300)
    except Exception:
        return out
    if r.returncode != 0 or not os.path.exists(tmp):
        return out
    for line in io.open(tmp, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 2:
            try:
                out[parts[0].strip().lower()] = int(parts[1])
            except ValueError:
                pass
    return out


def save_failed(d):
    """写回失败清单"""
    tmp = "/tmp/failed_to_od.txt"
    lines = ["# 云端切分/上传失败计数, 由 ty_pipeline 维护; >=%d 次不再处理" % FAIL_MAX]
    for k in sorted(d):
        lines.append("%s\t%d" % (k, d[k]))
    try:
        io.open(tmp, "w", encoding="utf-8").write("\n".join(lines) + "\n")
        rclone(["copyto", tmp, FAILED_MANIFEST], timeout=300)
        print("   失败清单已更新: %d 条" % len(d))
    except Exception as e:
        print("   失败清单写入异常: %s" % str(e)[:80])


def load_done():
    """拉已完成兜底清单 -> set(编号); 源文件 rename 失败时把编号记这里, 比对时纳入(防重复下载)"""
    tmp = "/tmp/ty_done_from_od.txt"
    out = set()
    try:
        r = rclone(["copyto", DONE_MANIFEST, tmp], timeout=300)
    except Exception:
        return out
    if r.returncode != 0 or not os.path.exists(tmp):
        return out
    try:
        for line in io.open(tmp, encoding="utf-8"):
            x = line.strip()
            if x and not x.startswith("#"):
                out.add(x.lower())
    except Exception:
        pass
    return out


def save_done(done):
    """写回已完成兜底清单"""
    if not done:
        return
    tmp = "/tmp/ty_done_to_od.txt"
    lines = ["# ty-transfer 已完成编号兜底(源文件 rename 失败时记, 比对时纳入, 防重复下载)",
             "# 更新: %s | 共 %d 个" % (time.strftime("%Y-%m-%d %H:%M:%S"), len(done))]
    for k in sorted(done):
        lines.append(k)
    try:
        io.open(tmp, "w", encoding="utf-8").write("\n".join(lines) + "\n")
        rclone(["copyto", tmp, DONE_MANIFEST], timeout=300)
        print("   已完成兜底清单已更新: %d 条" % len(done))
    except Exception as e:
        print("   已完成兜底清单写入异常: %s" % str(e)[:80])


def collect_have():
    """收集比对源编号: 仓库预导出清单(目标网盘) + rclone 实时读(中转远端/本机时含目标网盘)"""
    have = set()
    # 1) 优先从 中转远端/报告区 拉清单(本机 export_compare.py 生成并上传)
    tmp = "/tmp/compare_stems_from_od.txt"
    ok_list = False
    try:
        r = rclone(["copyto", COMPARE_OD_MANIFEST, tmp], timeout=600)
        if r.returncode == 0 and os.path.exists(tmp):
            cnt, stamp = 0, ""
            for line in io.open(tmp, encoding="utf-8"):
                x = line.strip()
                if x.startswith("#"):
                    if "更新时间" in x:
                        stamp = x.lstrip("# ").strip()
                    continue
                if x:
                    have.add(x.lower())
                    cnt += 1
            ok_list = cnt > 0
            print("   清单 %-26s %d 个编号 [%s]"
                  % (COMPARE_OD_MANIFEST, cnt, stamp or "无时间戳"))
    except Exception as e:
        print("   清单拉取异常: %s" % str(e)[:90])
    if not ok_list:
        print("   !! 目标网盘清单没拿到(%s) -> 目标网盘侧无法比对, 仅靠 中转远端 实时列表"
              % COMPARE_OD_MANIFEST)
        print("      请在本机双击 [导出比对清单.bat] 或让 worker 跑一轮后重试")

    # 2) 源远端/out 的编号清单(云端 out-sync 生成)
    tmp2 = "/tmp/gd_stems_from_od.txt"
    try:
        r2 = rclone(["copyto", GD_MANIFEST, tmp2], timeout=300)
        if r2.returncode == 0 and os.path.exists(tmp2):
            cnt2, stamp2 = 0, ""
            for line in io.open(tmp2, encoding="utf-8"):
                x = line.strip()
                if x.startswith("#"):
                    if "更新时间" in x:
                        stamp2 = x.lstrip("# ").strip()
                    continue
                if x:
                    have.add(x.lower())
                    cnt2 += 1
            print("   清单 %-26s %d 个编号 [源远端/out %s]"
                  % (GD_MANIFEST, cnt2, stamp2 or "无时间戳"))
        else:
            print("   清单 %-26s 暂不可用(源远端 还没归档过?)" % GD_MANIFEST)
    except Exception as e:
        print("   清单 %s 拉取异常: %s" % (GD_MANIFEST, str(e)[:80]))

    remotes = list(COMPARE_REMOTES)
    for r_ in COMPARE_REMOTES_LOCAL:      # 本机跑时 alist 可达, 顺带实时读
        remotes.append(r_)
    for remote in remotes:
        r = rclone(["lsf", remote], timeout=600)
        n = 0
        for line in (r.stdout or "").splitlines():
            name = line.strip()
            if name:
                have.add(stem(name))
                n += 1
        if r.returncode != 0 or n == 0:
            print("   比对源 %-28s 读不到(跳过)" % remote)
        else:
            print("   比对源 %-28s %d 个文件" % (remote, n))
    return have


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-files", type=int, default=2)
    ap.add_argument("--max-file-gb", type=float, default=15.0)
    ap.add_argument("--max-total-gb", type=float, default=10.0)
    ap.add_argument("--max-minutes", type=int, default=300)
    ap.add_argument("--max-mark", type=int, default=100, help="单轮最多自动标记多少个已存在编号")
    ap.add_argument("--only-name", default=os.environ.get("ONLY_NAME", ""),
                    help="只处理文件名含该子串的文件(指定/调试用)")
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    auth = os.environ.get("TY_AUTH", "")
    cid = os.environ.get("TY_CLOUD_ID", "")
    if not auth or not cid:
        print("!! 缺少 TY_AUTH / TY_CLOUD_ID")
        return 1
    c = TyCloud(auth, cid, parse_account(auth))
    # 安全: 云盘 的账号就是手机号, 绝不能进日志(仓库公开)
    print("云盘账号已载入 | cloud_id 长度 %d" % len(cid))
    os.makedirs(WORK, exist_ok=True)
    t0 = time.time()

    # 1) 找 in
    root = c.list_dir("")
    in_dir = next((x for x in root if x["is_dir"] and x["name"] == IN_NAME), None)
    if not in_dir:
        print("!! 根目录没有 %s 目录" % IN_NAME)
        return 1

    # 2) 比对源
    print("收集比对源编号 (目标网盘/out2 + 副网盘/out2 + 中转远端):")
    done = load_done()
    have = collect_have() | done
    if done:
        print("   清单 %-24s %d 个编号(已完成兜底, rename 失败也跳过)" % (DONE_MANIFEST, len(done)))
    print("   比对源编号总数: %d" % len(have))
    new_done = set()

    failed = load_failed()
    if failed:
        print("   失败清单: %d 个编号(其中 %d 个已达 %d 次, 将被跳过)"
              % (len(failed), sum(1 for v in failed.values() if v >= FAIL_MAX), FAIL_MAX))

    # 3) 分类
    files = [x for x in c.list_dir(in_dir["id"]) if not x["is_dir"]]
    limit_b = int(a.max_file_gb * 1024 ** 3)
    todo, dup, big, skipped, skip_failed = [], [], 0, 0, []
    for f in files:
        name = f["name"]
        if name.startswith(DONE_PREFIX):
            skipped += 1
            continue
        if not name.lower().endswith(VIDEO_EXT):
            continue
        if stem(name) in have:
            dup.append(f)
            continue
        if failed.get(stem(name), 0) >= FAIL_MAX:
            skip_failed.append(f)
            continue
        if f["size"] > limit_b:
            big += 1
            continue
        todo.append(f)
    if a.only_name:
        todo = [x for x in todo if a.only_name in x["name"]]
        print("按 --only-name 过滤后: %d 个" % len(todo))
    # 失败过的优先重试(否则永远排在"按大小升序"的队尾, 吃不到); 其次按大小升序
    todo.sort(key=lambda x: (0 if failed.get(stem(x["name"]), 0) > 0 else 1, x["size"]))
    print("in 共 %d 个 | 已标记 %d | 比对命中(重复) %d | 超 %.1fGB %d | 失败跳过 %d | 待处理 %d"
          % (len(files), skipped, len(dup), a.max_file_gb, big, len(skip_failed), len(todo)))
    if skip_failed:
        print("   已失败 %d 次不再上传: %s"
              % (FAIL_MAX, ", ".join(x["name"] for x in skip_failed[:5])))

    # 3.1 比对命中的直接标记"已下载_"(不下载)
    marked = 0
    for f in dup[:a.max_mark]:
        if stem(f["name"]) in done:
            continue                      # 上轮标记失败(已记兜底), 不再重试, 免无效调用
        if a.dry:
            print("   [dry] 将标记重复: %s" % f["name"])
            marked += 1
            continue
        r = c.rename(f["id"], DONE_PREFIX + f["name"], f["path"])
        if str(r.get("code")) == "0":
            marked += 1
            print("   ✔ 标记重复(已存在于比对源): %s" % f["name"])
        else:
            print("   !! 标记失败(记入兜底, 下轮跳过): %s: %s"
                  % (f["name"], json.dumps(r, ensure_ascii=False)[:110]))
            new_done.add(stem(f["name"]))
    if len(dup) > a.max_mark:
        print("   (本轮只标记前 %d 个, 余 %d 个下轮继续)" % (a.max_mark, len(dup) - a.max_mark))

    if not todo:
        print("没有待处理文件")
        return 0

    # 4) 挑本次处理的
    total_b, picked = 0, []
    for f in todo:
        if len(picked) >= a.max_files or total_b + f["size"] > a.max_total_gb * 1024 ** 3:
            break
        picked.append(f)
        total_b += f["size"]
    print("本次处理 %d 个 (%s): %s"
          % (len(picked), human(total_b), [x["name"] for x in picked]))
    if a.dry:
        print("(dry-run, 不实际执行)")
        return 0

    ffmpeg = shutil.which("ffmpeg") or "ffmpeg"
    ffprobe = shutil.which("ffprobe") or "ffprobe"

    ok_cnt = 0
    for f in picked:
        if (time.time() - t0) / 60 > a.max_minutes:
            print("!! 到达时间上限, 停止")
            break
        name = f["name"]
        print()
        print("=" * 62)
        print("处理: %s (%s)" % (name, human(f["size"])))

        # 磁盘空间前置检查(直链切分峰值约 1x 文件大小)
        try:
            free_b = shutil.disk_usage("/").free
        except Exception:
            free_b = 0
        need_b = f["size"] * 1.15
        if free_b and need_b > free_b:
            print("!! 磁盘不足: 可用 %s < 需要约 %s, 跳过该文件"
                  % (human(free_b), human(need_b)))
            continue

        workdir = os.path.join(WORK, re.sub(r"[^\w\u4e00-\u9fff.\-]", "_", name))
        shutil.rmtree(workdir, ignore_errors=True)
        parts_dir = os.path.join(workdir, "parts")
        os.makedirs(parts_dir, exist_ok=True)
        stem_ = os.path.splitext(name)[0]
        ext = os.path.splitext(name)[1]

        # 4.1 直链
        url, raw = c.get_link(f["id"], f["path"])
        if not url:
            print("!! 取直链失败: %s" % json.dumps(raw, ensure_ascii=False)[:140])
            continue
        print("直链 OK: %s..." % url[:70])
        url_remote = url          # 远端直链(整文件下载用); 之后 url 可能被换成本地路径

        # 4.2 上传助手: 逐段上传 -> 云端 _D -> 立即删本地
        def upload_parts(plist):
            for i, p in enumerate(plist, 1):
                base = os.path.basename(p)
                r_ = rclone(["copyto", p, OD_DEST + "/" + base, "--retries", "5",
                             "--retries-sleep", "10s", "--stats", "0"], timeout=7200)
                if r_.returncode != 0:
                    print("!! 上传失败 %s: %s" % (base, (r_.stderr or "")[:140]))
                    return False
                s2, e2 = os.path.splitext(base)
                rm = rclone(["moveto", OD_DEST + "/" + base,
                             OD_DEST + "/" + s2 + "_D" + e2], timeout=600)
                sz_mb = os.path.getsize(p) / 1048576
                os.remove(p)                      # 上传成功立即删本地分卷
                print("   %s (%.0f MB) 已上传+标记%s, 本地已删"
                      % (base, sz_mb, "OK" if rm.returncode == 0 else "失败"))
            return True

        def clean_parts():
            for p in glob.glob(os.path.join(parts_dir, stem_ + ".part*")):
                try:
                    os.remove(p)
                except OSError:
                    pass

        def fetch_local():
            """整文件下到本地(直链 HTTP 直读不稳/拿不到时长时兜底)。
            源盘下载 40~60MB/s, 代价小; 已下过且大小够就直接复用。"""
            lp = os.path.join(workdir, name)
            if os.path.exists(lp) and os.path.getsize(lp) >= f["size"] * 0.98:
                return lp
            print("   -> 整文件下载到本地 ...")
            t_dl = time.time()
            sh(["curl", "-sL", "-4", "--retry", "3", "--retry-delay", "5",
                "-o", lp, "--max-time", "7200", url_remote])
            got_b = os.path.getsize(lp) if os.path.exists(lp) else 0
            el = max(time.time() - t_dl, 0.1)
            print("      下载 %s / %.1f 分钟 (%.1f MB/s)"
                  % (human(got_b), el / 60, (got_b / 1048576) / el))
            return lp if got_b >= 1024 * 1024 else ""

        # 4.3 拿时长(分块/分段都用)
        need_split = f["size"] > MAX_PART
        dur = 0.0
        if need_split:
            for attempt in (1, 2, 3):
                try:
                    dur = float((sh([ffprobe, "-v", "error", "-show_entries",
                                     "format=duration", "-of", "csv=p=0", url],
                                    timeout=180).stdout or "0").strip() or 0)
                except Exception:
                    dur = 0.0
                if dur > 0:
                    break
                time.sleep(3)

        # 4.4 小于分卷目标: 整文件下载(不切)
        if not need_split:
            dst = os.path.join(parts_dir, name)
            t = time.time()
            sh(["curl", "-sL", "-4", "--retry", "3", "--retry-delay", "5",
                "-o", dst, "--max-time", "3600", url])
            got = os.path.getsize(dst) if os.path.exists(dst) else 0
            print("整文件下载: %s / %.1fs -> %.1f MB/s"
                  % (human(got), time.time() - t,
                     (got / 1048576) / max(time.time() - t, 0.1)))
            if got > 1024 * 1024 and upload_parts([dst]):
                r = c.rename(f["id"], DONE_PREFIX + name, f["path"])
                if str(r.get("code")) == "0":
                    print("✔ 源文件已标记: %s%s" % (DONE_PREFIX, name))
                    ok_cnt += 1
                else:
                    print("!! 源文件标记失败(记入兜底): %s" % json.dumps(r, ensure_ascii=False)[:120])
                    new_done.add(stem(name))
            else:
                print("!! 下载或上传失败, 保留源文件不动")
            shutil.rmtree(workdir, ignore_errors=True)
            continue

        # 4.5 需要切分: 分块处理, 每块单独"切分 -> 逐段上传 -> 删", 峰值磁盘 = 1 块
        BLOCK_BYTES = 3.5 * 1024 ** 3
        if not dur:
            # 兜底: 直链上 ffprobe 拿不到时长(CDN 不支持 seek / 容器缺 index)。
            #   注意: ffmpeg 的 segment muxer **没有** segment_size 选项(只有 *_time),
            #   所以"按大小切"这条路走不通, 必须先把时长问出来。
            #   做法: 整文件下到本地 -> 本地 ffprobe 可反复 seek -> 再本地切分。
            free_b = shutil.disk_usage("/").free
            if free_b and f["size"] * 2.2 > free_b:
                print("!! 兜底需先整文件落地(峰值磁盘约 %s), 可用 %s 不足, 跳过该文件"
                      % (human(f["size"] * 2.2), human(free_b)))
                failed[stem(name)] = failed.get(stem(name), 0) + 1
                shutil.rmtree(workdir, ignore_errors=True)
                continue
            lp = fetch_local()
            if not lp:
                print("!! 兜底: 整文件下载失败, 停止该文件")
                failed[stem(name)] = failed.get(stem(name), 0) + 1
                shutil.rmtree(workdir, ignore_errors=True)
                continue
            dur = probe_dur(ffprobe, lp) or probe_dur_packets(ffprobe, lp)
            if not dur:
                print("!! 兜底: 本地也拿不到时长(容器损坏/无视频流), 停止该文件")
                failed[stem(name)] = failed.get(stem(name), 0) + 1
                print("   失败计数: %s -> %d 次 (达 %d 次后不再处理)"
                      % (name, failed[stem(name)], FAIL_MAX))
                shutil.rmtree(workdir, ignore_errors=True)
                continue
            print("兜底: 本地探测时长 %.1f 秒 -> 改用本地文件切分" % dur)
            url = lp                  # 之后统一走本地文件(比 HTTP 直读稳)
        chunk = (f["size"] // 2) if (600 * 1024 ** 2) < f["size"] < (1024 ** 3) else SEG_TARGET
        seg = max(60, int(dur * chunk / f["size"])) if dur else 300
        n_blocks = max(1, int((f["size"] + BLOCK_BYTES - 1) // BLOCK_BYTES))
        print("分块处理: %d 块 x %s | 每段目标 %s 秒 (%.0f MB)"
              % (n_blocks, human(BLOCK_BYTES), seg, chunk / 1048576))

        pat = os.path.join(parts_dir, stem_ + ".part%03d" + ext)
        offset = 0
        all_ok = True
        for bi in range(n_blocks):
            ss = dur * bi / n_blocks
            blen = dur * (bi + 1) / n_blocks - ss
            got = []
            for src_try in (1, 2):
                for attempt in (1, 2):
                    args = [ffmpeg, "-y", "-v", "error"]
                    if bi > 0:
                        args += ["-ss", "%.3f" % ss]
                    args += ["-t", "%.3f" % blen]
                    args += ["-i", url, "-c", "copy", "-f", "segment",
                             "-segment_time", str(seg), "-reset_timestamps", "1",
                             "-segment_start_number", str(offset)]
                    if attempt > 1:
                        # 关键帧稀疏时按时长切不动 -> 允许在非关键帧处切
                        args += ["-break_non_keyframes", "1"]
                    args += [pat]
                    t = time.time()
                    r = sh(args, timeout=7200)
                    got = [p for p in sorted(glob.glob(os.path.join(parts_dir, stem_ + ".part*")))
                           if os.path.getsize(p) >= 1024 * 1024]
                    total = sum(os.path.getsize(p) for p in got)
                    print("第 %d/%d 块切分(第%d次): 退出码=%d 用时 %.1f 分钟 %d 段 %s"
                          % (bi + 1, n_blocks, attempt, r.returncode,
                             (time.time() - t) / 60, len(got), human(total)))
                    if r.returncode == 0 and got and all(
                            os.path.getsize(p) <= MAX_SEG_BYTES for p in got):
                        break
                    print("   失败: %s" % (r.stderr or "").strip()[:160])
                    clean_parts()
                    got = []
                    seg = max(30, int(seg * 0.6))     # 段太大(关键帧稀疏) -> 收小再试
                if got:
                    break
                # 直连 HTTP 直读切不出来(常见: 读到一半连接被截断) -> 整文件落地后重试本块
                if src_try == 1 and url == url_remote:
                    if not fetch_local():
                        break
                    url = os.path.join(workdir, name)   # 之后所有块都走本地文件
                    print("   直连切分失败 -> 改用本地文件重试本块")
                    continue
                break
            if not got:
                print("!! 第 %d 块切分失败, 停止该文件(不上传整文件)" % (bi + 1))
                failed[stem(name)] = failed.get(stem(name), 0) + 1
                print("   失败计数: %s -> %d 次 (达 %d 次后不再处理)"
                      % (name, failed[stem(name)], FAIL_MAX))
                all_ok = False
                break
            if not upload_parts(got):
                print("!! 第 %d 块上传中断, 停止该文件" % (bi + 1))
                failed[stem(name)] = failed.get(stem(name), 0) + 1
                all_ok = False
                break
            offset += len(got)
            print("   >> 第 %d/%d 块完成 (累计上传 %d 段)" % (bi + 1, n_blocks, offset))

        if not all_ok:
            shutil.rmtree(workdir, ignore_errors=True)
            continue

        # 4.4 全部上传完 -> 标记源文件
        r = c.rename(f["id"], DONE_PREFIX + name, f["path"])
        if str(r.get("code")) == "0":
            print("✔ 源文件已标记: %s%s" % (DONE_PREFIX, name))
            ok_cnt += 1
        else:
            print("!! 源文件标记失败(记入兜底, 下轮不再重传): %s"
                  % json.dumps(r, ensure_ascii=False)[:130])
            new_done.add(stem(name))
        shutil.rmtree(workdir, ignore_errors=True)

    print()
    print("=" * 62)
    print("本轮: 完成 %d 个文件 | 标记重复 %d 个 | 用时 %.1f 分钟"
          % (ok_cnt, marked, (time.time() - t0) / 60))
    print(sh(["df", "-h", "/"]).stdout.strip().splitlines()[-1])
    if failed:
        save_failed(failed)
    if new_done:
        done |= new_done
        save_done(done)
    return 0


if __name__ == "__main__":
    import logmask          # 日志脱敏: 文件名/路径/手机号 -> 标签(见 logmask.py)
    logmask.install()
    sys.exit(main())
