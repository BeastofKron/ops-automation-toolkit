#!/usr/bin/env python3
"""
organize_bookmarks.py — check & reorganize a browser bookmarks export.

Reads a Netscape-format bookmarks HTML file (the export format used by
Chrome, Edge, Firefox, Brave, Opera, Vivaldi, and Safari), optionally
verifies each link is still alive, sorts the bookmarks into folders by
content type, and writes a new bookmarks HTML file ready to re-import.

Usage
-----
    python organize_bookmarks.py INPUT.html [OUTPUT.html] [options]

Options
    --no-check          Skip live URL checking (much faster)
    --workers N         Parallel link-check workers (default: 32)
    --timeout SECONDS   Per-URL timeout (default: 8)
    --report FILE.csv   Write a CSV report (default: bookmarks_report.csv
                        next to the output file)
    --keep-broken       Include broken links in a "Broken / Unreachable"
                        folder (default: include but separated)
    --drop-broken       Omit broken links entirely from the output
    --verbose           Print each URL as it's checked

Examples
    python organize_bookmarks.py favorites.html
    python organize_bookmarks.py favorites.html organized.html --workers 64
    python organize_bookmarks.py favorites.html --no-check
"""

from __future__ import annotations

import argparse
import csv
import html
import os
import re
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import urllib.request
import urllib.error


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Bookmark:
    url: str
    title: str
    folder_path: str          # original folder path, slash-separated
    add_date: str = ""
    icon: str = ""            # data: URI from the original file (preserved)
    status: str = "unchecked" # "ok" | "redirect" | "broken" | "timeout" | "unchecked"
    status_code: Optional[int] = None
    final_url: Optional[str] = None
    error: str = ""
    category: str = "Uncategorized"


# ---------------------------------------------------------------------------
# Parser — Netscape bookmark format
# ---------------------------------------------------------------------------
#
# The format is a loose, indented HTML-ish thing. The reliable signals are:
#   <DT><H3 ...>FolderName</H3>     -> start of a folder (a <DL> follows)
#   <DL><p>                          -> begin folder contents
#   </DL><p>                         -> end folder contents
#   <DT><A HREF="..." ...>Title</A> -> a single bookmark
#
# Some exporters omit the trailing <p> or use slightly different casing.
# We match case-insensitively.

_RE_FOLDER = re.compile(r"<DT>\s*<H3\b([^>]*)>(.*?)</H3>", re.IGNORECASE | re.DOTALL)
_RE_LINK = re.compile(r"<DT>\s*<A\b([^>]*)>(.*?)</A>", re.IGNORECASE | re.DOTALL)
_RE_END_FOLDER = re.compile(r"</DL>", re.IGNORECASE)
_RE_ATTR = re.compile(r'(\w+)\s*=\s*"([^"]*)"')


def _attrs(attrs_blob: str) -> dict[str, str]:
    return {m.group(1).upper(): m.group(2) for m in _RE_ATTR.finditer(attrs_blob)}


def parse_bookmarks(path: Path) -> list[Bookmark]:
    """Walk the file line by line so we can maintain folder context."""
    text = path.read_text(encoding="utf-8", errors="replace")

    # Build a single stream of "events" in document order: folder-start,
    # folder-end, link.
    events: list[tuple[str, int, re.Match]] = []
    for m in _RE_FOLDER.finditer(text):
        events.append(("folder", m.start(), m))
    for m in _RE_LINK.finditer(text):
        events.append(("link", m.start(), m))
    for m in _RE_END_FOLDER.finditer(text):
        events.append(("end", m.start(), m))
    events.sort(key=lambda e: e[1])

    stack: list[str] = []
    out: list[Bookmark] = []

    for kind, _pos, m in events:
        if kind == "folder":
            name = html.unescape(m.group(2)).strip()
            stack.append(name)
        elif kind == "end":
            if stack:
                stack.pop()
        else:  # link
            attrs = _attrs(m.group(1))
            url = attrs.get("HREF", "").strip()
            if not url:
                continue
            title = html.unescape(m.group(2)).strip()
            out.append(
                Bookmark(
                    url=url,
                    title=title or url,
                    folder_path="/".join(stack) if stack else "",
                    add_date=attrs.get("ADD_DATE", ""),
                    icon=attrs.get("ICON", ""),
                )
            )

    return out


# ---------------------------------------------------------------------------
# Categorizer
# ---------------------------------------------------------------------------
#
# Strategy: explicit domain → category map first, then keyword heuristics on
# the URL path and title. Everything that misses falls into "Uncategorized"
# so you can review and either re-bucket or extend the map below.

DOMAIN_CATEGORY: dict[str, str] = {
    # News & Media
    "nytimes.com": "News & Media", "wsj.com": "News & Media",
    "washingtonpost.com": "News & Media", "bbc.com": "News & Media",
    "bbc.co.uk": "News & Media", "cnn.com": "News & Media",
    "reuters.com": "News & Media", "apnews.com": "News & Media",
    "npr.org": "News & Media", "theguardian.com": "News & Media",
    "bloomberg.com": "News & Media", "axios.com": "News & Media",
    "politico.com": "News & Media", "economist.com": "News & Media",
    "ft.com": "News & Media", "vox.com": "News & Media",
    "theverge.com": "News & Media", "arstechnica.com": "News & Media",
    "techcrunch.com": "News & Media", "wired.com": "News & Media",
    "engadget.com": "News & Media", "cnet.com": "News & Media",

    # Shopping
    "amazon.com": "Shopping", "ebay.com": "Shopping",
    "etsy.com": "Shopping", "walmart.com": "Shopping",
    "target.com": "Shopping", "costco.com": "Shopping",
    "bestbuy.com": "Shopping", "homedepot.com": "Shopping",
    "lowes.com": "Shopping", "ikea.com": "Shopping",
    "wayfair.com": "Shopping", "newegg.com": "Shopping",
    "bhphotovideo.com": "Shopping", "aliexpress.com": "Shopping",
    "temu.com": "Shopping", "shein.com": "Shopping",

    # Social & Communication
    "facebook.com": "Social", "instagram.com": "Social",
    "x.com": "Social", "twitter.com": "Social", "threads.net": "Social",
    "linkedin.com": "Social", "reddit.com": "Social",
    "tiktok.com": "Social", "pinterest.com": "Social",
    "discord.com": "Social", "slack.com": "Social",
    "whatsapp.com": "Social", "mastodon.social": "Social",
    "bsky.app": "Social",

    # Tools & Productivity
    "google.com": "Tools & Productivity",
    "docs.google.com": "Tools & Productivity",
    "drive.google.com": "Tools & Productivity",
    "mail.google.com": "Tools & Productivity",
    "calendar.google.com": "Tools & Productivity",
    "outlook.com": "Tools & Productivity",
    "office.com": "Tools & Productivity",
    "office365.com": "Tools & Productivity",
    "notion.so": "Tools & Productivity",
    "trello.com": "Tools & Productivity",
    "asana.com": "Tools & Productivity",
    "monday.com": "Tools & Productivity",
    "todoist.com": "Tools & Productivity",
    "evernote.com": "Tools & Productivity",
    "dropbox.com": "Tools & Productivity",
    "onedrive.live.com": "Tools & Productivity",
    "icloud.com": "Tools & Productivity",
    "zoom.us": "Tools & Productivity",
    "meet.google.com": "Tools & Productivity",
    "calendly.com": "Tools & Productivity",
    "1password.com": "Tools & Productivity",
    "lastpass.com": "Tools & Productivity",
    "bitwarden.com": "Tools & Productivity",

    # Development & Tech
    "github.com": "Development & Tech",
    "gitlab.com": "Development & Tech",
    "bitbucket.org": "Development & Tech",
    "stackoverflow.com": "Development & Tech",
    "stackexchange.com": "Development & Tech",
    "developer.mozilla.org": "Development & Tech",
    "mdn.dev": "Development & Tech",
    "npmjs.com": "Development & Tech",
    "pypi.org": "Development & Tech",
    "docker.com": "Development & Tech",
    "hub.docker.com": "Development & Tech",
    "kubernetes.io": "Development & Tech",
    "aws.amazon.com": "Development & Tech",
    "console.aws.amazon.com": "Development & Tech",
    "cloud.google.com": "Development & Tech",
    "azure.microsoft.com": "Development & Tech",
    "portal.azure.com": "Development & Tech",
    "raspberrypi.org": "Development & Tech",
    "raspberrypi.com": "Development & Tech",
    "arduino.cc": "Development & Tech",
    "anthropic.com": "Development & Tech",
    "claude.ai": "Development & Tech",
    "openai.com": "Development & Tech",
    "chatgpt.com": "Development & Tech",
    "huggingface.co": "Development & Tech",

    # Reference & Docs
    "wikipedia.org": "Reference & Docs",
    "wiktionary.org": "Reference & Docs",
    "archive.org": "Reference & Docs",
    "merriam-webster.com": "Reference & Docs",
    "dictionary.com": "Reference & Docs",
    "thesaurus.com": "Reference & Docs",
    "wolframalpha.com": "Reference & Docs",

    # Entertainment & Video
    "youtube.com": "Entertainment & Video",
    "youtu.be": "Entertainment & Video",
    "netflix.com": "Entertainment & Video",
    "hulu.com": "Entertainment & Video",
    "disneyplus.com": "Entertainment & Video",
    "max.com": "Entertainment & Video",
    "hbomax.com": "Entertainment & Video",
    "primevideo.com": "Entertainment & Video",
    "peacocktv.com": "Entertainment & Video",
    "paramountplus.com": "Entertainment & Video",
    "appletv.com": "Entertainment & Video",
    "tv.apple.com": "Entertainment & Video",
    "twitch.tv": "Entertainment & Video",
    "vimeo.com": "Entertainment & Video",
    "imdb.com": "Entertainment & Video",
    "rottentomatoes.com": "Entertainment & Video",
    "letterboxd.com": "Entertainment & Video",

    # Music
    "spotify.com": "Music", "open.spotify.com": "Music",
    "music.apple.com": "Music", "music.youtube.com": "Music",
    "soundcloud.com": "Music", "bandcamp.com": "Music",
    "pandora.com": "Music", "tidal.com": "Music",
    "last.fm": "Music", "genius.com": "Music",

    # Gaming
    "steampowered.com": "Gaming", "store.steampowered.com": "Gaming",
    "steamcommunity.com": "Gaming",
    "epicgames.com": "Gaming", "gog.com": "Gaming",
    "xbox.com": "Gaming", "playstation.com": "Gaming",
    "nintendo.com": "Gaming", "ign.com": "Gaming",
    "gamespot.com": "Gaming", "polygon.com": "Gaming",
    "kotaku.com": "Gaming", "rockpapershotgun.com": "Gaming",
    "speedrun.com": "Gaming", "twitch.tv/directory": "Gaming",
    "retropie.org.uk": "Gaming",

    # Finance & Banking
    "chase.com": "Finance", "bankofamerica.com": "Finance",
    "wellsfargo.com": "Finance", "citi.com": "Finance",
    "usbank.com": "Finance", "capitalone.com": "Finance",
    "americanexpress.com": "Finance", "discover.com": "Finance",
    "fidelity.com": "Finance", "schwab.com": "Finance",
    "vanguard.com": "Finance", "robinhood.com": "Finance",
    "etrade.com": "Finance", "tdameritrade.com": "Finance",
    "mint.com": "Finance", "ynab.com": "Finance",
    "paypal.com": "Finance", "venmo.com": "Finance",
    "cash.app": "Finance", "coinbase.com": "Finance",
    "irs.gov": "Finance", "turbotax.intuit.com": "Finance",
    "hrblock.com": "Finance",

    # Travel
    "kayak.com": "Travel", "expedia.com": "Travel",
    "booking.com": "Travel", "airbnb.com": "Travel",
    "vrbo.com": "Travel", "hotels.com": "Travel",
    "tripadvisor.com": "Travel", "trivago.com": "Travel",
    "google.com/travel": "Travel", "maps.google.com": "Travel",
    "openstreetmap.org": "Travel",
    "delta.com": "Travel", "united.com": "Travel",
    "aa.com": "Travel", "southwest.com": "Travel",
    "marriott.com": "Travel", "hilton.com": "Travel",
    "hyatt.com": "Travel", "ihg.com": "Travel",

    # Health & Fitness
    "webmd.com": "Health & Fitness", "mayoclinic.org": "Health & Fitness",
    "healthline.com": "Health & Fitness", "medlineplus.gov": "Health & Fitness",
    "cdc.gov": "Health & Fitness", "nih.gov": "Health & Fitness",
    "myfitnesspal.com": "Health & Fitness", "strava.com": "Health & Fitness",
    "fitbit.com": "Health & Fitness", "garmin.com": "Health & Fitness",
    "peloton.com": "Health & Fitness",

    # Food & Cooking
    "allrecipes.com": "Food & Cooking", "foodnetwork.com": "Food & Cooking",
    "epicurious.com": "Food & Cooking", "seriouseats.com": "Food & Cooking",
    "bonappetit.com": "Food & Cooking", "nytimes.com/recipes": "Food & Cooking",
    "cooking.nytimes.com": "Food & Cooking", "kingarthurbaking.com": "Food & Cooking",
    "yelp.com": "Food & Cooking", "doordash.com": "Food & Cooking",
    "ubereats.com": "Food & Cooking", "grubhub.com": "Food & Cooking",
    "opentable.com": "Food & Cooking",

    # Education & Learning
    "khanacademy.org": "Education", "coursera.org": "Education",
    "edx.org": "Education", "udemy.com": "Education",
    "udacity.com": "Education", "duolingo.com": "Education",
    "brilliant.org": "Education", "ted.com": "Education",
    "academia.edu": "Education", "scholar.google.com": "Education",

    # Government & Civic
    "usa.gov": "Government & Civic", "irs.gov": "Government & Civic",
    "ssa.gov": "Government & Civic", "usps.com": "Government & Civic",
    "uscis.gov": "Government & Civic", "dmv.org": "Government & Civic",
    "vote.gov": "Government & Civic",
}

# Keyword → category. Applied to lowercased URL + title.
KEYWORD_CATEGORY: list[tuple[str, str]] = [
    ("github.io", "Development & Tech"),
    ("readthedocs", "Development & Tech"),
    ("dev.to", "Development & Tech"),
    ("medium.com", "Reference & Docs"),
    ("substack.com", "News & Media"),
    ("news", "News & Media"),
    ("blog", "Reference & Docs"),
    ("recipe", "Food & Cooking"),
    ("cooking", "Food & Cooking"),
    ("workout", "Health & Fitness"),
    ("fitness", "Health & Fitness"),
    ("yoga", "Health & Fitness"),
    ("gym", "Health & Fitness"),
    ("travel", "Travel"),
    ("flight", "Travel"),
    ("hotel", "Travel"),
    ("itinerary", "Travel"),
    ("airport", "Travel"),
    ("login", "Tools & Productivity"),
    ("account", "Tools & Productivity"),
    ("dashboard", "Tools & Productivity"),
    ("billing", "Finance"),
    ("invoice", "Finance"),
    ("bank", "Finance"),
    ("retro", "Gaming"),
    ("emulator", "Gaming"),
    ("rom", "Gaming"),
    ("xbox", "Gaming"),
    ("playstation", "Gaming"),
    ("nintendo", "Gaming"),
    ("steam", "Gaming"),
    ("podcast", "Entertainment & Video"),
    ("music", "Music"),
    ("album", "Music"),
    ("playlist", "Music"),
    ("tutorial", "Reference & Docs"),
    ("how to", "Reference & Docs"),
    ("guide", "Reference & Docs"),
    ("wiki", "Reference & Docs"),
    ("doc", "Reference & Docs"),
    ("api", "Development & Tech"),
    ("sdk", "Development & Tech"),
    ("python", "Development & Tech"),
    ("javascript", "Development & Tech"),
    ("react", "Development & Tech"),
    ("docker", "Development & Tech"),
    ("kubernetes", "Development & Tech"),
    ("raspberry", "Development & Tech"),
    ("arduino", "Development & Tech"),
    ("home depot", "Shopping"),
    ("shop", "Shopping"),
    ("store", "Shopping"),
    ("buy", "Shopping"),
    ("amazon", "Shopping"),
    ("ebay", "Shopping"),
]


def _root_domain(host: str) -> str:
    host = host.lower().lstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def categorize(b: Bookmark) -> str:
    if b.status == "broken":
        return "Broken / Unreachable"
    try:
        parsed = urlparse(b.url)
    except Exception:
        return "Uncategorized"
    host = _root_domain(parsed.hostname or "")
    # exact domain match
    if host in DOMAIN_CATEGORY:
        return DOMAIN_CATEGORY[host]
    # subdomain match: walk up the dotted parts
    parts = host.split(".")
    for i in range(1, len(parts)):
        suffix = ".".join(parts[i:])
        if suffix in DOMAIN_CATEGORY:
            return DOMAIN_CATEGORY[suffix]
    # path-qualified domain (e.g. google.com/travel)
    hostpath = (host + parsed.path).lower()
    for dom, cat in DOMAIN_CATEGORY.items():
        if "/" in dom and hostpath.startswith(dom):
            return cat
    # keyword fallback on URL + title
    blob = (b.url + " " + b.title).lower()
    for kw, cat in KEYWORD_CATEGORY:
        if kw in blob:
            return cat
    # local network / IP addresses go to a special bucket
    if parsed.hostname and re.match(r"^[\d\.:]+$", parsed.hostname):
        return "Local Network"
    if parsed.hostname and parsed.hostname.endswith(".local"):
        return "Local Network"
    return "Uncategorized"


# ---------------------------------------------------------------------------
# Link checker
# ---------------------------------------------------------------------------

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

_ssl_context = ssl.create_default_context()
# Some older sites have weak/broken TLS; we don't want a TLS error to mark
# them "broken" — we still got a server, just an unhappy one.
_ssl_context.check_hostname = False
_ssl_context.verify_mode = ssl.CERT_NONE


def check_url(url: str, timeout: float) -> tuple[str, Optional[int], Optional[str], str]:
    """
    Returns (status, status_code, final_url, error_message).
    status in {"ok", "redirect", "broken", "timeout", "skipped"}.
    """
    try:
        parsed = urlparse(url)
    except Exception as e:
        return "broken", None, None, f"unparseable URL: {e}"

    # Skip non-http(s) schemes (file://, javascript:, chrome://, etc.)
    if parsed.scheme not in ("http", "https"):
        return "skipped", None, None, f"scheme={parsed.scheme}"

    # Local / private network — don't try over the public internet.
    if parsed.hostname and (
        re.match(r"^(127\.|10\.|192\.168\.|169\.254\.)", parsed.hostname)
        or parsed.hostname in ("localhost",)
        or parsed.hostname.endswith(".local")
    ):
        return "skipped", None, None, "local network"

    # Try HEAD first (cheap). Fall back to GET because many servers reject HEAD.
    for method in ("HEAD", "GET"):
        req = urllib.request.Request(url, method=method, headers={
            "User-Agent": UA,
            "Accept": "*/*",
        })
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=_ssl_context) as resp:
                code = resp.getcode()
                final = resp.geturl()
                redirected = (final != url)
                if 200 <= code < 400:
                    return ("redirect" if redirected else "ok"), code, final, ""
                return "broken", code, final, f"HTTP {code}"
        except urllib.error.HTTPError as e:
            # 4xx/5xx are reachable but unhappy. 405 (HEAD not allowed) → retry GET.
            if method == "HEAD" and e.code in (400, 403, 405, 501):
                continue
            return "broken", e.code, None, f"HTTP {e.code}"
        except urllib.error.URLError as e:
            reason = getattr(e, "reason", e)
            if isinstance(reason, TimeoutError) or "timed out" in str(reason).lower():
                return "timeout", None, None, "timeout"
            # DNS / connection / TLS failures: try the next method, otherwise fail.
            if method == "HEAD":
                continue
            return "broken", None, None, str(reason)
        except TimeoutError:
            return "timeout", None, None, "timeout"
        except Exception as e:
            if method == "HEAD":
                continue
            return "broken", None, None, str(e)

    return "broken", None, None, "exhausted methods"


def check_all(bookmarks: list[Bookmark], workers: int, timeout: float, verbose: bool) -> None:
    start = time.monotonic()
    done = 0
    total = len(bookmarks)
    print(f"Checking {total} URLs with {workers} workers (timeout={timeout}s)...", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(check_url, b.url, timeout): b for b in bookmarks}
        for fut in as_completed(futures):
            b = futures[fut]
            status, code, final, err = fut.result()
            b.status = status
            b.status_code = code
            b.final_url = final
            b.error = err
            done += 1
            if verbose:
                print(f"  [{done}/{total}] {status:8} {b.url}", file=sys.stderr)
            elif done % 25 == 0 or done == total:
                elapsed = time.monotonic() - start
                rate = done / elapsed if elapsed else 0
                eta = (total - done) / rate if rate else 0
                print(f"  ...{done}/{total}  ({rate:.1f}/s, ~{eta:.0f}s left)", file=sys.stderr)


# ---------------------------------------------------------------------------
# Output: Netscape bookmarks HTML
# ---------------------------------------------------------------------------

def _esc(s: str) -> str:
    return html.escape(s, quote=True)


def write_bookmarks_html(
    bookmarks: list[Bookmark],
    out_path: Path,
    drop_broken: bool = False,
) -> dict[str, int]:
    """Group by .category and write a Netscape-format file."""
    by_cat: dict[str, list[Bookmark]] = {}
    for b in bookmarks:
        if drop_broken and b.category == "Broken / Unreachable":
            continue
        by_cat.setdefault(b.category, []).append(b)

    # Stable, friendly ordering. Anything not listed is appended alphabetically;
    # "Broken" and "Uncategorized" sink to the bottom.
    preferred = [
        "News & Media", "Social", "Tools & Productivity",
        "Development & Tech", "Reference & Docs",
        "Entertainment & Video", "Music", "Gaming",
        "Shopping", "Finance", "Travel",
        "Health & Fitness", "Food & Cooking", "Education",
        "Government & Civic", "Local Network",
    ]
    other = sorted(c for c in by_cat if c not in preferred and c not in
                   ("Uncategorized", "Broken / Unreachable"))
    order = [c for c in preferred if c in by_cat] + other
    if "Uncategorized" in by_cat:
        order.append("Uncategorized")
    if "Broken / Unreachable" in by_cat:
        order.append("Broken / Unreachable")

    now = int(time.time())
    lines: list[str] = []
    lines.append("<!DOCTYPE NETSCAPE-Bookmark-file-1>")
    lines.append("<!-- Generated by organize_bookmarks.py -->")
    lines.append('<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">')
    lines.append("<TITLE>Bookmarks</TITLE>")
    lines.append("<H1>Bookmarks</H1>")
    lines.append("<DL><p>")
    lines.append(f'    <DT><H3 ADD_DATE="{now}" LAST_MODIFIED="{now}" PERSONAL_TOOLBAR_FOLDER="true">Favorites bar</H3>')
    lines.append("    <DL><p>")

    counts: dict[str, int] = {}
    for cat in order:
        items = by_cat[cat]
        counts[cat] = len(items)
        lines.append(f'        <DT><H3 ADD_DATE="{now}" LAST_MODIFIED="{now}">{_esc(cat)} ({len(items)})</H3>')
        lines.append("        <DL><p>")
        # Sort within a category: alphabetical by title for predictability.
        for b in sorted(items, key=lambda x: x.title.lower()):
            attrs = [f'HREF="{_esc(b.url)}"']
            if b.add_date:
                attrs.append(f'ADD_DATE="{_esc(b.add_date)}"')
            if b.icon:
                attrs.append(f'ICON="{_esc(b.icon)}"')
            note = ""
            if b.status == "broken":
                note = f"  [BROKEN: {_esc(b.error or str(b.status_code))}]"
            elif b.status == "timeout":
                note = "  [TIMEOUT]"
            elif b.status == "redirect" and b.final_url:
                note = "  [redirects]"
            lines.append(
                f'            <DT><A {" ".join(attrs)}>{_esc(b.title)}{note}</A>'
            )
        lines.append("        </DL><p>")

    lines.append("    </DL><p>")
    lines.append("</DL><p>")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return counts


def write_report_csv(bookmarks: list[Bookmark], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "title", "url", "category", "status", "status_code",
            "final_url", "error", "original_folder",
        ])
        for b in bookmarks:
            w.writerow([
                b.title, b.url, b.category, b.status,
                b.status_code if b.status_code is not None else "",
                b.final_url or "", b.error, b.folder_path,
            ])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Check & reorganize a browser bookmarks export.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="See the top of the script for full usage examples.",
    )
    ap.add_argument("input", type=Path, help="Bookmarks HTML export")
    ap.add_argument("output", type=Path, nargs="?",
                    help="Output HTML (default: <input>_organized.html)")
    ap.add_argument("--no-check", action="store_true",
                    help="Skip live URL checking")
    ap.add_argument("--workers", type=int, default=32,
                    help="Parallel link-check workers (default: 32)")
    ap.add_argument("--timeout", type=float, default=8.0,
                    help="Per-URL timeout seconds (default: 8)")
    ap.add_argument("--report", type=Path, default=None,
                    help="CSV report path (default: alongside output)")
    ap.add_argument("--drop-broken", action="store_true",
                    help="Omit broken links from output")
    ap.add_argument("--verbose", action="store_true",
                    help="Print every URL as it's checked")
    args = ap.parse_args(argv)

    if not args.input.exists():
        print(f"error: {args.input} not found", file=sys.stderr)
        return 2

    out = args.output or args.input.with_name(args.input.stem + "_organized.html")
    report = args.report or out.with_name(out.stem + "_report.csv")

    print(f"Reading {args.input} ...", file=sys.stderr)
    bookmarks = parse_bookmarks(args.input)
    print(f"  parsed {len(bookmarks)} bookmarks", file=sys.stderr)

    if not args.no_check:
        check_all(bookmarks, workers=args.workers, timeout=args.timeout,
                  verbose=args.verbose)
    else:
        print("Skipping live URL check (--no-check)", file=sys.stderr)

    for b in bookmarks:
        b.category = categorize(b)

    counts = write_bookmarks_html(bookmarks, out, drop_broken=args.drop_broken)
    write_report_csv(bookmarks, report)

    # Summary to stdout
    print()
    print(f"Wrote {out}")
    print(f"Wrote {report}")
    print()
    print(f"{'Category':32}  Count")
    print(f"{'-'*32}  -----")
    for cat, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"{cat:32}  {n:>5}")
    if not args.no_check:
        broken = sum(1 for b in bookmarks if b.status == "broken")
        timed_out = sum(1 for b in bookmarks if b.status == "timeout")
        ok = sum(1 for b in bookmarks if b.status in ("ok", "redirect"))
        skipped = sum(1 for b in bookmarks if b.status == "skipped")
        print()
        print(f"Link check: {ok} alive, {broken} broken, {timed_out} timed out, "
              f"{skipped} skipped (local/non-http).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
