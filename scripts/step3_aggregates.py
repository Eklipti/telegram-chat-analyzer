# Telegram Chat Analytics Pipeline
# Copyright (C) 2025 Eklipti
#
# Этот проект — свободное программное обеспечение: вы можете
# распространять и/или изменять его на условиях
# Стандартной общественной лицензии GNU (GNU GPL)
# третьей версии, опубликованной Фондом свободного ПО.
#
# Программа распространяется в надежде, что она будет полезной,
# но БЕЗ КАКИХ-ЛИБО ГАРАНТИЙ; даже без подразумеваемой гарантии
# ТОВАРНОГО СОСТОЯНИЯ или ПРИГОДНОСТИ ДЛЯ КОНКРЕТНОЙ ЦЕЛИ.
# Подробности см. в Стандартной общественной лицензии GNU.
#
# Вы должны были получить копию Стандартной общественной
# лицензии GNU вместе с этой программой. Если это не так,
# см. <https://www.gnu.org/licenses/>.

from __future__ import annotations
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Optional
from . import utils
from datetime import datetime

def build_aggregates_json(input_0: Path, out_dir: Path) -> None:
    data = utils.load_json(input_0)
    msgs = data.get("messages") or []
    chat_id = data.get("id", "unknown_chat_id")
    out_dir.mkdir(parents=True, exist_ok=True)

    by_day: Dict[str,int] = Counter()
    by_hour: Dict[int,int] = Counter()
    by_author: Dict[str,int] = Counter()
    name_by_id: Dict[str,str] = {}
    message_ids: set[int] = set()
    reply_ids: set[int] = set()
    edited_ids: set[int] = set()
    react_ids: set[int] = set()
    media_ids: set[int] = set()
    photo_ids: set[int] = set()
    gif_ids: set[int] = set()
    other_media_ids: set[int] = set()
    id_to_parent: Dict[int, Optional[int]] = {}
    id_to_msg: Dict[int, Dict[str, Any]] = {}
    root_cache: Dict[int, int] = {}
    thread_size: Dict[int, int] = Counter()
    thread_participants: Dict[int, set] = defaultdict(set)
    emoji_counter: Counter = Counter()
    top_reacted_msgs: list[tuple[int,int]] = []
    reactions_by_author: Dict[str,int] = Counter()
    media_counter: Dict[str,int] = Counter()
    polls_by_author: Dict[str,int] = Counter()

    for m in msgs:
        msg_type = m.get("type")
        if msg_type != "message":
            continue
        
        meta = m.get("meta_norm", {})
        mid = m.get("id")
        if isinstance(mid, int):
            message_ids.add(mid)
            id_to_msg[mid] = m
            pid = m.get("reply_to_message_id")
            id_to_parent[mid] = pid if isinstance(pid, int) else None
            if isinstance(pid, int):
                reply_ids.add(mid)

        if "edited" in m or "edited_unixtime" in m or meta.get("edited_norm"):
            if isinstance(mid, int):
                edited_ids.add(mid)

        if "reactions" in m:
            if isinstance(mid, int):
                react_ids.add(mid)

        media_cat = meta.get("media_cat")
        if media_cat is not None:
            if isinstance(mid, int):
                media_ids.add(mid)
                if media_cat == "photo":
                    photo_ids.add(mid)
                elif media_cat == "animation (GIF)":
                    gif_ids.add(mid)
                else:
                    other_media_ids.add(mid)
            if isinstance(media_cat, str):
                media_counter[media_cat] += 1
        if media_cat == "poll":
            fid = m.get("from_id")
            if fid:
                polls_by_author[str(fid)] += 1

        fid = m.get("from_id")
        if fid:
            fid_str = str(fid)
            by_author[fid_str] += 1
            disp = m.get("from")
            if isinstance(disp, str) and disp.strip():
                name_by_id[fid_str] = disp

        dn = meta.get("date_norm")
        if isinstance(dn, str) and len(dn) >= 10:
            by_day[dn[:10]] += 1
            try:
                dt_obj = datetime.fromisoformat(dn)
                h = dt_obj.hour
                by_hour[h] += 1
            except Exception:
                pass

        reactions = m.get("reactions")
        total_r_for_msg = 0
        if isinstance(reactions, list):
            for r in reactions:
                if not isinstance(r, dict): continue
                cnt = r.get("count", 0)
                try: cnt = int(cnt)
                except Exception: cnt = 0
                key = r.get("emoji") or r.get("type") or "?"
                emoji_counter[key] += cnt
                total_r_for_msg += cnt

        if fid and total_r_for_msg:
            reactions_by_author[str(fid)] += total_r_for_msg
        if isinstance(mid, int):
            top_reacted_msgs.append((total_r_for_msg, mid))

    def find_root(x: int) -> int:
        seen = []
        while True:
            p = id_to_parent.get(x)
            if p is None or p not in id_to_parent: r = x; break
            if x in root_cache: r = root_cache[x]; break
            seen.append(x); x = p
            if len(seen) > 1000: r = x; break
        for y in seen: root_cache[y] = r
        return r

    for m in msgs:
        if m.get("type") != "message":
            continue
        mid = m.get("id")
        fid = m.get("from_id")
        if not isinstance(mid, int): continue
        root = find_root(mid)
        thread_size[root] += 1
        if fid:
            thread_participants[root].add(str(fid))
    
    top_authors_list = [
        {"from_id": fid, "username": name_by_id.get(fid, fid), "count": cnt}
        for fid, cnt in sorted(by_author.items(), key=lambda x: x[1], reverse=True)[:10]
    ]
    
    threads_top5_list = []
    threads_top = sorted(thread_size.items(), key=lambda x: x[1], reverse=True)[:5]
    for root_id, size in threads_top:
        rm = id_to_msg.get(root_id, {})
        rm_meta = rm.get("meta_norm", {})
        threads_top5_list.append({
            "root_id": root_id, "size": int(size),
            "username": rm.get("from") or (str(rm.get("from_id")) if rm.get("from_id") else ""),
            "date_norm": rm_meta.get("date_norm") or rm.get("date") or "",
            "text_preview": (rm_meta.get("text_plain") or "")[:140],
            "unique_participants": len(thread_participants.get(root_id, set())),
        })

    emoji_top5_list = [{"emoji": k, "count": int(v)} for k, v in emoji_counter.most_common(5)]

    top_reacted_msgs_list = []
    top_reacted_msgs.sort(key=lambda x: x[0], reverse=True)
    for total_r, mid in top_reacted_msgs[:3]:
        m = id_to_msg.get(mid, {})
        m_meta = m.get("meta_norm", {})
        top_reacted_msgs_list.append({
            "id": mid, "reactions_total": int(total_r),
            "username": m.get("from") or (str(m.get("from_id")) if m.get("from_id") else ""),
            "date_norm": m_meta.get("date_norm") or m.get("date") or "",
            "text_preview": (m_meta.get("text_plain") or "")[:140],
        })

    reactions_by_author_top5_list = [
        {"from_id": fid, "username": name_by_id.get(fid, fid), "total_reactions": int(cnt)}
        for fid, cnt in sorted(reactions_by_author.items(), key=lambda x: x[1], reverse=True)[:5]
    ]

    def pct(n: int, tot: int) -> float:
        return round((n / tot * 100.0), 2) if tot else 0.0
    
    total = len(message_ids)
    replies_count = len(reply_ids)
    edited_count = len(edited_ids)
    react_msgs_count = len(react_ids)
    media_msgs_count = len(media_ids)
    
    photo_count = len(photo_ids)
    gif_count = len(gif_ids)
    other_media_count = len(other_media_ids)
    
    media_shares_dict = {
        k: {"count": int(v), "pct": pct(int(v), media_msgs_count)}
        for k, v in media_counter.items()
    }

    poll_creators_top3_list = [
        {"from_id": fid, "username": name_by_id.get(fid, fid), "polls": int(cnt)}
        for fid, cnt in sorted(polls_by_author.items(), key=lambda x: x[1], reverse=True)[:3]
    ]

    final_aggregates = {
        "chat_id": chat_id,
        "source_file_path": str(input_0.resolve()),
        "source_file_name": input_0.name,
        "generation_timestamp": int(datetime.now().timestamp()),
        
        "summary": {
            "total_messages": total,
            "replies": {
                "count": replies_count,
                "pct": pct(replies_count, total)
            },
            "edited_msgs": edited_count,
            "messages_with_reactions": {
                "count": react_msgs_count,
                "pct": pct(react_msgs_count, total)
            },
            "media": {
                "count": media_msgs_count,
                "pct": pct(media_msgs_count, total)
            },
            "media_breakdown": {
                "photo": {
                    "count": photo_count,
                    "pct": pct(photo_count, media_msgs_count)
                },
                "gif": {
                    "count": gif_count,
                    "pct": pct(gif_count, media_msgs_count)
                },
                "other": {
                    "count": other_media_count,
                    "pct": pct(other_media_count, media_msgs_count)
                }
            }
        },
        "by_day": dict(sorted(by_day.items())),
        "by_hour": dict(sorted(by_hour.items())),
        
        "top_authors": top_authors_list,
        "threads_top5": threads_top5_list,
        "emoji_top5": emoji_top5_list,
        "top_reacted_messages_top3": top_reacted_msgs_list,
        "reactions_by_author_top5": reactions_by_author_top5_list,
        "media_shares": media_shares_dict,
        "poll_creators_top3": poll_creators_top3_list
    }

    utils.save_json(out_dir / "all_aggregates.json", final_aggregates)