import re
import time
from datetime import datetime
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType
from apscheduler.triggers.cron import CronTrigger


class EmbyTagEnhance(_PluginBase):
    plugin_name = "Emby标签增强"
    plugin_desc = "通过豆瓣标签和TMDB关键词丰富Emby影视分类，无需Cookie也可使用"
    plugin_icon = "tag.png"
    plugin_version = "1.0.0"
    plugin_author = "zhangxuanqing"
    author_url = ""
    plugin_config_prefix = "embytagenhance_"
    plugin_order = 50
    auth_level = 1

    _enabled = False
    _emby_url = ""
    _emby_api_key = ""
    _douban_cookie = ""
    _tag_prefix = "db:"
    _scan_cron = "0 3 * * *"
    _scan_mode = "incremental"
    _min_tag_count = 5
    _tag_blacklist = "电影,电视剧,好看,影视,推荐,剧集,影片,视频"
    _tag_mapping = ""
    _dry_run = False
    _request_interval = 5
    _tag_source = "auto"

    _progress = None
    _stats = None
    _running = False
    _lock = Lock()

    _emby_user_id = ""

    def _resolve_media_server(self) -> Tuple[str, str]:
        try:
            from app.helper.mediaserver import MediaServerHelper
            services = MediaServerHelper().get_services(type_filter="emby")
            for svc in services:
                if svc.instance and svc.config:
                    host = svc.config.get("host", "").rstrip("/")
                    apikey = svc.config.get("apikey", "")
                    if host and apikey:
                        return host, apikey
        except Exception as e:
            logger.debug(f"从MediaServerHelper获取Emby配置失败: {e}")
        return "", ""

    def _get_emby_admin_user(self) -> str:
        if self._emby_user_id:
            return self._emby_user_id
        try:
            resp = requests.get(
                f"{self._emby_url}/emby/Users",
                params={"api_key": self._emby_api_key},
                timeout=10,
            )
            resp.raise_for_status()
            for u in resp.json():
                if u.get("Policy", {}).get("IsAdministrator"):
                    self._emby_user_id = u["Id"]
                    return self._emby_user_id
        except Exception as e:
            logger.error(f"获取Emby管理员用户失败: {e}")
        return ""

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._emby_url = (config.get("emby_url") or "").rstrip("/")
        self._emby_api_key = config.get("emby_api_key") or ""
        if not self._emby_url or not self._emby_api_key:
            auto_url, auto_key = self._resolve_media_server()
            if auto_url and auto_key:
                if not self._emby_url:
                    self._emby_url = auto_url
                if not self._emby_api_key:
                    self._emby_api_key = auto_key
                logger.info(f"Emby标签增强: 自动检测到Emby配置 {self._emby_url}")
        self._douban_cookie = config.get("douban_cookie") or ""
        self._tag_prefix = config.get("tag_prefix") or "db:"
        self._scan_cron = config.get("scan_cron") or "0 3 * * *"
        self._scan_mode = config.get("scan_mode") or "incremental"
        self._min_tag_count = int(config.get("min_tag_count") or 5)
        self._tag_blacklist = config.get("tag_blacklist") or "电影,电视剧,好看,影视,推荐,剧集,影片,视频"
        self._tag_mapping = config.get("tag_mapping") or ""
        self._dry_run = bool(config.get("dry_run"))
        self._request_interval = int(config.get("request_interval") or 5)
        self._tag_source = config.get("tag_source") or "auto"

        self._progress = self.get_data("progress") or {
            "total": 0,
            "processed": 0,
            "skipped": 0,
            "failed": 0,
        }
        self._stats = self.get_data("stats") or {
            "total_tags_added": 0,
            "top_tags": [],
            "last_scan_time": "",
            "last_scan_duration": 0,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/emby_tag_scan",
                "event": EventType.PluginAction,
                "desc": "触发Emby标签增强扫描",
                "category": "插件命令",
                "data": {"action": "emby_tag_scan"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/scan",
                "endpoint": self.api_scan,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "手动触发扫描",
            },
            {
                "path": "/progress",
                "endpoint": self.api_progress,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "获取扫描进度",
            },
            {
                "path": "/stats",
                "endpoint": self.api_stats,
                "methods": ["GET"],
                "auth": "apikey",
                "summary": "获取统计数据",
            },
            {
                "path": "/preview",
                "endpoint": self.api_preview,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "预览影片标签",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._scan_cron)
        except Exception:
            trigger = CronTrigger.from_crontab("0 3 * * *")
        return [
            {
                "id": "EmbyTagEnhance.Scan",
                "name": "Emby标签增强定时扫描",
                "trigger": trigger,
                "func": self.scan_and_tag,
                "kwargs": {},
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "emby_url",
                                            "label": "Emby地址（留空自动读取）",
                                            "placeholder": "http://192.168.1.1:8096",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "emby_api_key",
                                            "label": "Emby API Key（留空自动读取）",
                                            "type": "password",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "tag_source",
                                            "label": "标签数据源",
                                            "items": [
                                                {"title": "自动（优先豆瓣用户标签，回退内置API）", "value": "auto"},
                                                {"title": "仅豆瓣用户标签（需Cookie）", "value": "douban_web"},
                                                {"title": "仅MoviePilot内置API（无需Cookie）", "value": "mp_builtin"},
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "douban_cookie",
                                            "label": "豆瓣Cookie（用户标签模式需要）",
                                            "placeholder": "bid=xxx; dbcl2=xxx",
                                            "rows": 2,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "tag_prefix",
                                            "label": "标签前缀",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "scan_cron",
                                            "label": "扫描周期（Cron表达式）",
                                            "placeholder": "0 3 * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "scan_mode",
                                            "label": "扫描模式",
                                            "items": [
                                                {"title": "增量扫描", "value": "incremental"},
                                                {"title": "全量扫描", "value": "full"},
                                            ],
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "min_tag_count",
                                            "label": "标签最少标记人数",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "request_interval",
                                            "label": "请求间隔（秒）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "dry_run",
                                            "label": "预览模式（不实际写入）",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "tag_blacklist",
                                            "label": "标签黑名单（逗号分隔）",
                                            "placeholder": "电影,电视剧,好看",
                                            "rows": 2,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "tag_mapping",
                                            "label": "标签映射（每行一条：原标签→新标签）",
                                            "placeholder": "赛博朋克→科幻未来\n黑色幽默→暗黑喜剧",
                                            "rows": 3,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "emby_url": "",
            "emby_api_key": "",
            "douban_cookie": "",
            "tag_prefix": "db:",
            "scan_cron": "0 3 * * *",
            "scan_mode": "incremental",
            "min_tag_count": 5,
            "tag_blacklist": "电影,电视剧,好看,影视,推荐,剧集,影片,视频",
            "tag_mapping": "",
            "dry_run": False,
            "request_interval": 5,
            "tag_source": "auto",
        }

    def get_page(self) -> List[dict]:
        progress = self._progress or {}
        stats = self._stats or {}
        top_tags = stats.get("top_tags", [])[:10]
        top_tags_text = ""
        for item in top_tags:
            top_tags_text += f"{item['name']}  {item['count']}部\n"

        return [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"variant": "tonal"},
                                "content": [
                                    {
                                        "component": "VCardTitle",
                                        "props": {"text": "扫描进度"},
                                    },
                                    {
                                        "component": "VCardText",
                                        "props": {
                                            "text": f"总计: {progress.get('total', 0)} 部\n已处理: {progress.get('processed', 0)} 部\n跳过: {progress.get('skipped', 0)} 部\n失败: {progress.get('failed', 0)} 部",
                                        },
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"variant": "tonal"},
                                "content": [
                                    {
                                        "component": "VCardTitle",
                                        "props": {"text": "标签统计"},
                                    },
                                    {
                                        "component": "VCardText",
                                        "props": {
                                            "text": f"累计新增标签: {stats.get('total_tags_added', 0)} 个\n上次扫描: {stats.get('last_scan_time', '未运行')}\n耗时: {stats.get('last_scan_duration', 0)} 秒",
                                        },
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VCard",
                                "props": {"variant": "tonal"},
                                "content": [
                                    {
                                        "component": "VCardTitle",
                                        "props": {"text": "Top 标签"},
                                    },
                                    {
                                        "component": "VCardText",
                                        "props": {
                                            "text": top_tags_text or "暂无数据",
                                        },
                                    },
                                ],
                            }
                        ],
                    },
                ],
            },
        ]

    def get_dashboard(self, key: str = None, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        if not self._enabled:
            return None
        progress = self._progress or {}
        stats = self._stats or {}
        top_tags = stats.get("top_tags", [])[:5]
        tag_lines = "\n".join([f"  {t['name']}  {t['count']}部" for t in top_tags])

        col_config = {"cols": 12, "md": 6}
        global_config = {
            "title": "Emby标签增强",
            "refresh": 60,
            "border": True,
        }
        page = [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": f"已处理 {progress.get('processed', 0)}/{progress.get('total', 0)} 部 | 新增标签 {stats.get('total_tags_added', 0)} 个\n{tag_lines}",
                },
            }
        ]
        return col_config, global_config, page

    def stop_service(self):
        self._running = False

    # ==================== Emby API ====================

    _HESTRIP = {"Chapters", "MediaSources", "MediaStreams", "People", "BackdropImageTags",
                "ChapterInfo", "MediaAttachments"}

    def _emby_request(self, method: str, path: str, params: dict = None, json_data: dict = None) -> Optional[Any]:
        if not self._emby_url or not self._emby_api_key:
            logger.error("Emby地址或API Key未配置")
            return None
        url = f"{self._emby_url}/emby{path}"
        params = params or {}
        params["api_key"] = self._emby_api_key
        try:
            resp = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                timeout=30,
            )
            resp.raise_for_status()
            if resp.status_code == 204:
                return {"_status": 204}
            if resp.text:
                return resp.json()
            return {}
        except Exception as e:
            logger.error(f"Emby API请求失败: {url}, 错误: {e}")
            return None

    def _get_emby_items(self, item_types: str = "Movie,Series") -> List[dict]:
        user_id = self._get_emby_admin_user()
        fields = "ProviderIds,TagItems,Genres,Overview,ProductionYear,ProductionLocations"
        if user_id:
            result = self._emby_request(
                "GET",
                f"/Users/{user_id}/Items",
                params={
                    "Fields": fields,
                    "IncludeItemTypes": item_types,
                    "Recursive": "true",
                    "Limit": 10000,
                },
            )
        else:
            result = self._emby_request(
                "GET",
                "/Items",
                params={
                    "Fields": fields,
                    "IncludeItemTypes": item_types,
                    "Recursive": "true",
                    "Limit": 10000,
                },
            )
        if not result:
            return []
        return result.get("Items", [])

    def _get_emby_item(self, item_id: str) -> Optional[dict]:
        user_id = self._get_emby_admin_user()
        if user_id:
            result = self._emby_request(
                "GET",
                f"/Users/{user_id}/Items/{item_id}",
                params={
                    "Fields": "ProviderIds,TagItems,Genres",
                },
            )
            if result and "Id" in result:
                return result
        result = self._emby_request(
            "GET",
            "/Items",
            params={
                "Fields": "ProviderIds,TagItems,Genres",
                "Ids": item_id,
            },
        )
        if not result:
            return None
        items = result.get("Items", [])
        return items[0] if items else None

    def _update_emby_item_tags(self, item_id: str, new_tag_names: List[str]) -> bool:
        item = self._get_emby_item(item_id)
        if not item:
            logger.error(f"获取Emby项目失败: {item_id}")
            return False
        existing_tag_items = item.get("TagItems") or []
        existing_names = {t["Name"] for t in existing_tag_items}
        merged = list(existing_tag_items)
        for name in new_tag_names:
            if name not in existing_names:
                merged.append({"Name": name, "Id": 0})
                existing_names.add(name)
        item["TagItems"] = merged
        return self._post_emby_item(item)

    def _post_emby_item(self, item: dict) -> bool:
        item_id = item.get("Id", "")
        for key in self._HESTRIP:
            item.pop(key, None)
        result = self._emby_request(
            "POST",
            f"/Items/{item_id}",
            json_data=item,
        )
        if result is not None:
            return True
        return False

    # ==================== Douban Tags ====================

    def _get_douban_id_by_name(self, name: str, mtype: str = None) -> Optional[str]:
        try:
            resp = requests.get(
                "https://movie.douban.com/j/subject_suggest",
                params={"q": name},
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/120.0.0.0 Safari/537.36",
                },
                timeout=10,
            )
            if resp.status_code == 200:
                results = resp.json()
                target_type = "movie" if mtype == "Movie" else "tv"
                for r in results:
                    if r.get("type") == target_type and r.get("id"):
                        return str(r["id"])
                if results and results[0].get("id"):
                    return str(results[0]["id"])
        except Exception as e:
            logger.debug(f"豆瓣名称搜索失败 ({name}): {e}")
        return None

    def _fetch_emby_native_tags(self, item: dict) -> List[dict]:
        tags = []
        genres = item.get("Genres") or []
        for g in genres:
            tags.append({"name": g, "count": 9999, "source": "emby_genre"})
        locations = item.get("ProductionLocations") or []
        for loc in locations:
            tags.append({"name": loc, "count": 9999, "source": "emby_country"})
        return tags

    def _fetch_douban_tags_web(self, douban_id: str) -> List[dict]:
        if not douban_id:
            return []
        url = f"https://movie.douban.com/j/subject/{douban_id}/tags"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": f"https://movie.douban.com/subject/{douban_id}/",
        }
        if self._douban_cookie:
            headers["Cookie"] = self._douban_cookie
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("tags", [])
            else:
                logger.debug(f"豆瓣网页标签接口返回 {resp.status_code}: {douban_id}")
        except Exception as e:
            logger.debug(f"获取豆瓣网页标签失败 {douban_id}: {e}")
        return []

    def _fetch_tags_via_mp_builtin(self, item: dict) -> List[dict]:
        tags = self._fetch_emby_native_tags(item)
        item_name = item.get("Name", "")
        item_type = item.get("Type", "")
        douban_id = self._get_douban_id_by_name(item_name, item_type)
        if douban_id:
            time.sleep(self._request_interval)
            web_tags = self._fetch_douban_tags_web(douban_id)
            if web_tags:
                tags.extend(web_tags)
        return tags

    def _fetch_douban_tags(self, item: dict) -> List[dict]:
        item_name = item.get("Name", "")
        item_type = item.get("Type", "")
        source = self._tag_source

        if source == "mp_builtin":
            return self._fetch_tags_via_mp_builtin(item)

        if source == "douban_web":
            douban_id = self._get_douban_id_by_name(item_name, item_type)
            if douban_id:
                time.sleep(self._request_interval)
                web_tags = self._fetch_douban_tags_web(douban_id)
                if web_tags:
                    return web_tags
            if not self._douban_cookie:
                logger.warning("豆瓣用户标签模式需要配置Cookie且回退到内置API")
            return self._fetch_tags_via_mp_builtin(item)

        douban_id = self._get_douban_id_by_name(item_name, item_type)
        if douban_id:
            time.sleep(self._request_interval)
            web_tags = self._fetch_douban_tags_web(douban_id)
            if web_tags:
                return web_tags

        logger.debug("豆瓣用户标签获取失败，回退到内置标签")
        return self._fetch_tags_via_mp_builtin(item)

    # ==================== Tag Processing ====================

    def _parse_tag_mapping(self) -> Dict[str, str]:
        mapping = {}
        if not self._tag_mapping:
            return mapping
        for line in self._tag_mapping.strip().split("\n"):
            line = line.strip()
            if "→" in line:
                parts = line.split("→", 1)
                if len(parts) == 2:
                    src = parts[0].strip()
                    dst = parts[1].strip()
                    if src and dst:
                        mapping[src] = dst
            elif "->" in line:
                parts = line.split("->", 1)
                if len(parts) == 2:
                    src = parts[0].strip()
                    dst = parts[1].strip()
                    if src and dst:
                        mapping[src] = dst
        return mapping

    def _get_blacklist(self) -> set:
        if not self._tag_blacklist:
            return set()
        return set(t.strip() for t in self._tag_blacklist.split(",") if t.strip())

    def _filter_tags(self, raw_tags: List[dict]) -> List[str]:
        blacklist = self._get_blacklist()
        mapping = self._parse_tag_mapping()
        seen_genres = set()
        result = []
        for tag_item in raw_tags:
            name = tag_item.get("name", "").strip()
            count = int(tag_item.get("count", 0))
            source = tag_item.get("source", "")
            if not name:
                continue
            if source in ("emby_genre", "douban_genre") and name in seen_genres:
                continue
            if source in ("emby_genre", "douban_genre"):
                seen_genres.add(name)
            if source not in ("emby_genre", "emby_country", "douban_genre",
                              "douban_country", "tmdb_keyword"):
                if count < self._min_tag_count:
                    continue
            if name in blacklist:
                continue
            if re.match(r"^[\d.]+$", name):
                continue
            if len(name) > 10:
                continue
            mapped = mapping.get(name, name)
            prefixed = f"{self._tag_prefix}{mapped}"
            if prefixed not in result:
                result.append(prefixed)
        return result

    def _merge_tags(self, existing_tag_items: list, new_tags: List[str]) -> List[str]:
        existing_names = set()
        for t in (existing_tag_items or []):
            name = t.get("Name", "") if isinstance(t, dict) else str(t)
            existing_names.add(name)
        result = list(existing_names)
        for tag in new_tags:
            if tag not in existing_names:
                result.append(tag)
                existing_names.add(tag)
        return result

    # ==================== Main Scan Logic ====================

    def scan_and_tag(self):
        with self._lock:
            if self._running:
                logger.info("Emby标签增强: 扫描任务正在运行中，跳过")
                return
            self._running = True

        start_time = time.time()
        try:
            if not self._emby_url or not self._emby_api_key:
                logger.error("Emby标签增强: 请先配置Emby地址和API Key")
                return

            logger.info("Emby标签增强: 开始扫描媒体库...")
            items = self._get_emby_items()
            if not items:
                logger.warning("Emby标签增强: 未获取到媒体项")
                return

            processed_ids = set()
            if self._scan_mode == "incremental":
                processed_ids = set(self.get_data("processed_ids") or [])

            self._progress = {
                "total": len(items),
                "processed": 0,
                "skipped": 0,
                "failed": 0,
            }

            tag_counter = {}
            total_tags_added = 0
            new_processed_ids = set(processed_ids)

            for idx, item in enumerate(items):
                if not self._running:
                    logger.info("Emby标签增强: 扫描被中断")
                    break

                item_id = item.get("Id", "")
                item_name = item.get("Name", "Unknown")
                item_type = item.get("Type", "")

                if self._scan_mode == "incremental" and item_id in processed_ids:
                    self._progress["skipped"] += 1
                    continue

                existing_tag_items = item.get("TagItems") or []
                existing_tag_names = {t["Name"] for t in existing_tag_items if isinstance(t, dict)}
                if any(n.startswith(self._tag_prefix) for n in existing_tag_names):
                    logger.debug(f"跳过(已有插件标签): {item_name}")
                    self._progress["skipped"] += 1
                    continue

                raw_tags = self._fetch_douban_tags(item)

                if not raw_tags:
                    logger.debug(f"跳过(无标签数据): {item_name}")
                    self._progress["skipped"] += 1
                    continue

                filtered_tags = self._filter_tags(raw_tags)
                if not filtered_tags:
                    self._progress["skipped"] += 1
                    continue

                existing_tag_items = item.get("TagItems") or []
                existing_tag_names = {t["Name"] for t in existing_tag_items if isinstance(t, dict)}
                tags_to_add = [t for t in filtered_tags if t not in existing_tag_names]
                tags_added = len(tags_to_add)

                if tags_added > 0:
                    for tag in tags_to_add:
                        tag_name = tag[len(self._tag_prefix):] if tag.startswith(self._tag_prefix) else tag
                        tag_counter[tag_name] = tag_counter.get(tag_name, 0) + 1

                    if self._dry_run:
                        logger.info(f"[预览] {item_name}: 将添加标签 {tags_to_add}")
                        total_tags_added += tags_added
                    else:
                        success = self._update_emby_item_tags(item_id, tags_to_add)
                        if success:
                            logger.info(f"已更新: {item_name} (+{tags_added} 标签)")
                            total_tags_added += tags_added
                        else:
                            logger.error(f"写入失败: {item_name}")
                            self._progress["failed"] += 1
                            continue

                self._progress["processed"] += 1
                new_processed_ids.add(item_id)

                if (idx + 1) % 20 == 0:
                    self.save_data("progress", self._progress)
                    self.save_data("processed_ids", list(new_processed_ids))

            duration = round(time.time() - start_time, 1)
            sorted_tags = sorted(tag_counter.items(), key=lambda x: x[1], reverse=True)
            self._stats = {
                "total_tags_added": total_tags_added,
                "top_tags": [{"name": k, "count": v} for k, v in sorted_tags[:20]],
                "last_scan_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_scan_duration": duration,
            }

            self.save_data("progress", self._progress)
            self.save_data("stats", self._stats)
            self.save_data("processed_ids", list(new_processed_ids))

            logger.info(
                f"Emby标签增强: 扫描完成! "
                f"处理 {self._progress['processed']} 部, "
                f"跳过 {self._progress['skipped']} 部, "
                f"失败 {self._progress['failed']} 部, "
                f"新增标签 {total_tags_added} 个, "
                f"耗时 {duration}s"
            )

        except Exception as e:
            logger.error(f"Emby标签增强: 扫描异常 - {e}")
        finally:
            self._running = False

    # ==================== API Endpoints ====================

    def api_scan(self, request: dict = None) -> dict:
        if self._running:
            return {"status": "running", "message": "扫描任务正在运行中"}
        from threading import Thread
        Thread(target=self.scan_and_tag, daemon=True).start()
        return {"status": "started", "message": "扫描任务已启动"}

    def api_progress(self, request: dict = None) -> dict:
        return {
            "progress": self._progress or {},
            "running": self._running,
        }

    def api_stats(self, request: dict = None) -> dict:
        return self._stats or {}

    def api_preview(self, request: dict = None) -> dict:
        request = request or {}
        item_id = request.get("item_id")
        if not item_id:
            return {"error": "请提供 item_id"}

        item = self._get_emby_item(item_id)
        if not item:
            return {"error": "未找到该媒体项"}

        item_name = item.get("Name", "")
        raw_tags = self._fetch_douban_tags(item)
        filtered_tags = self._filter_tags(raw_tags)
        existing_tag_items = item.get("TagItems") or []
        existing_tag_names = [t["Name"] for t in existing_tag_items if isinstance(t, dict)]
        tags_to_add = [t for t in filtered_tags if t not in existing_tag_names]

        return {
            "name": item_name,
            "raw_tags": raw_tags,
            "filtered_tags": filtered_tags,
            "existing_tags": existing_tag_names,
            "tags_to_add": tags_to_add,
        }

    # ==================== Event Handler ====================

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event):
        event_data = event.event_data or {}
        if event_data.get("action") != "emby_tag_scan":
            return
        if not self._enabled:
            return
        logger.info("Emby标签增强: 收到远程扫描命令")
        self.scan_and_tag()
