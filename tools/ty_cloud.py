# -*- coding: utf-8 -*-
"""云盘 API 客户端 (纯标准库, 从 alist 对应驱动复刻)
支持: 列表 / 取直链 / 改名
"""
import base64
import hashlib
import json
import random
import string
import time
import urllib.error
import urllib.parse
import urllib.request

import pathcfg          # API 地址从配置读, 不在源码里写死
BASE = pathcfg.require("TT_BASE")


def open_retry(op, req, timeout, tries=4):
    """网络抖动重试(2026-10-26 加)。

    起因: runner 侧偶发 `socket.gaierror: [Errno -3] Temporary failure in name
    resolution`, 一次 DNS 抽风就把整轮 12 小时的搬运打成 failure(#70/#51)。
    这里只在「请求大概率没送达」的错误上重试(DNS/连不上/超时/5xx),
    4xx 一律立刻抛(令牌失效、路径不对, 重试没意义)。
    POST 的幂等性: list/getLink 是查询; rename 改的是同一个目标名, 重复执行等价。
    """
    last = None
    for i in range(tries):
        try:
            return op.open(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code not in (408, 429) and e.code < 500:
                raise
            last = e
        except (urllib.error.URLError, OSError) as e:
            last = e
        if i < tries - 1:
            wait = 3 * (2 ** i)
            print("   !! 网络异常(%s), %ds 后重试(%d/%d)"
                  % (str(last)[:110], wait, i + 1, tries - 1), flush=True)
            time.sleep(wait)
    raise last


def cal_sign(body, ts, rand):
    """复刻 alist calSign"""
    b = urllib.parse.quote(body, safe="").replace("+", "%20")
    b = "".join(sorted(b))
    b = base64.b64encode(b.encode()).decode()
    m1 = hashlib.md5(b.encode()).hexdigest()
    m2 = hashlib.md5(("%s:%s" % (ts, rand)).encode()).hexdigest()
    return hashlib.md5((m1 + m2).encode()).hexdigest().upper()


def parse_account(authorization):
    """authorization 形如 base64('pc:账号:...') -> 账号"""
    try:
        raw = base64.b64decode(authorization + "===").decode("utf-8", "replace")
        for sep in (":", "|"):
            parts = raw.split(sep)
            for p in parts:
                if p.isdigit() and len(p) >= 11:
                    return p
        return parts[1] if len(parts) > 1 else ""
    except Exception:
        return ""


def pick(d, *keys, default=None):
    """大小写不敏感取值 (接口返回小写开头, 上游源码里是 Go 风格大写)"""
    if not isinstance(d, dict):
        return default
    low = {k.lower(): v for k, v in d.items()}
    for k in keys:
        if k in d:
            return d[k]
        if k.lower() in low:
            return low[k.lower()]
    return default


class TyCloud:
    def __init__(self, authorization, cloud_id, account):
        self.auth = authorization
        self.cloud_id = cloud_id
        self.account = account or parse_account(authorization)
        self.op = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def _post(self, path, data):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        rand = "".join(random.choices(string.ascii_letters + string.digits, k=16))
        body = json.dumps(data, ensure_ascii=False)
        sign = cal_sign(body, ts, rand)
        req = urllib.request.Request(
            BASE + path, method="POST", data=body.encode("utf-8"),
            headers={
                "Accept": "application/json, text/plain, */*",
                "Authorization": "Basic " + self.auth,
                "CMS-DEVICE": "default",
                "mcloud-channel": "1000101",
                "mcloud-client": "10701",
                "mcloud-sign": "%s,%s,%s" % (ts, rand, sign),
                "mcloud-version": "7.14.0",
                "Origin": BASE,
                "Referer": BASE + "/w/",
                "x-DeviceInfo": "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||",
                "Content-Type": "application/json",
            })
        with open_retry(self.op, req, timeout=90) as r:
            return json.loads(r.read().decode("utf-8", "replace"))

    def new_json(self, data):
        d = dict(data)
        d.update({"catalogType": 3, "cloudID": self.cloud_id, "cloudType": 1,
                  "commonAccountInfo": {"account": self.account, "accountType": 1}})
        return d

    def list_dir(self, catalog_id, page_size=100):
        out, page = [], 1
        while page <= 50:
            data = self.new_json({
                "catalogID": catalog_id, "contentSortType": 0,
                "pageInfo": {"pageNum": page, "pageSize": page_size},
                "sortDirection": 1})
            r = self._post("/orchestration/familyCloud-rebuild/content/v1.2/queryContentList", data)
            if str(r.get("code")) not in ("0", "None") and r.get("success") is False:
                raise RuntimeError("列表失败: %s" % json.dumps(r, ensure_ascii=False)[:200])
            d = r.get("data") or {}
            dirs = pick(d, "catalogList", "CloudCatalogList", default=[]) or []
            files = pick(d, "contentList", "CloudContentList", default=[]) or []
            for c in dirs:
                out.append({"id": pick(c, "catalogID", "catalogId"),
                            "name": pick(c, "catalogName"),
                            "is_dir": True, "size": 0,
                            "path": pick(d, "path", default="/") or "/"})
            for c in files:
                out.append({"id": pick(c, "contentID", "contentId"),
                            "name": pick(c, "contentName"),
                            "is_dir": False,
                            "size": int(pick(c, "contentSize", default=0) or 0),
                            "path": pick(d, "path", default="/") or "/"})
            if not dirs and not files:
                break
            page += 1
        return out

    def get_link(self, content_id, path):
        data = self.new_json({"contentID": content_id, "path": path})
        r = self._post("/orchestration/familyCloud-rebuild/content/v1.0/getFileDownLoadURL", data)
        return (pick(r.get("data") or {}, "downloadURL", "downloadUrl", default="") or ""), r

    def rename(self, content_id, new_name, path):
        data = {"contentID": content_id, "contentName": new_name,
                "commonAccountInfo": {"account": self.account, "accountType": 1},
                "path": path}
        return self._post("/orchestration/familyCloud-rebuild/photoContent/v1.0/modifyContentInfo", data)
