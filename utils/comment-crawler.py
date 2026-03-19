"""
Facebook Comment Crawler (Playwright)
=====================================
Uses a real browser to crawl comments from Facebook posts.
Fetches about 100 comments per batch and waits between batches.

Usage:
    Create a .env file in the project root with:
        c_user=your_c_user_cookie
        xs=your_xs_cookie
    Then run:
    python utils/comment-crawler.py

Install:
    pip install playwright
    playwright install chromium
"""

import os
import random
import re
import signal
import time
from datetime import datetime

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Missing dependency. Install with:")
    print("pip install playwright")
    print("playwright install chromium")
    raise SystemExit(1)


# Configuration
BATCH_SIZE = 100
WAIT_MINUTES = 1
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data")
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

running = True
stop_requests = 0

SKIP_EXACT_LINES = {
    "Like",
    "Reply",
    "Share",
    "Edited",
    "Top fan",
    "Author",
    "Most relevant",
    "All comments",
    "Top comments",
    "Newest",
    "GIPHY",
    "GIF",
    "Follow",
    "See more",
    "See less",
    "View more replies",
    "View previous replies",
    "See translation",
    "Translated from",
    "Write a comment...",
    "Write a public comment...",
    "Write a reply...",
    "Write a public reply...",
    "Send stars",
    "Send star",
    "·",
}

SKIP_LINE_PATTERNS = (
    re.compile(r"^\d+\s*[smhdwy]$", re.IGNORECASE),
    re.compile(r"^\d+\s*(seconds?|minutes?|hours?|days?|weeks?|years?)\s*ago$", re.IGNORECASE),
    re.compile(r"^about an?\s+(second|minute|hour|day|week|year).*$", re.IGNORECASE),
    re.compile(r"^\d+$"),
    re.compile(r"^(view|see)\s+\d+\s+more\s+(replies|reply|comments|comment)$", re.IGNORECASE),
    re.compile(r"^\d+\s+(replies|reply|comments|comment)$", re.IGNORECASE),
    re.compile(r"^write\s+(a|your)\s+(public\s+)?reply", re.IGNORECASE),
    re.compile(r"^write\s+(a|your)\s+(public\s+)?comment", re.IGNORECASE),
    re.compile(r"^translated from", re.IGNORECASE),
    re.compile(r"^send\s+(a\s+)?star", re.IGNORECASE),
)


def signal_handler(sig, frame):
    global running, stop_requests
    stop_requests += 1
    if stop_requests == 1:
        print("\n\nStopping soon... Press Ctrl+C again to force quit.")
        running = False
        return

    raise KeyboardInterrupt


def interruptible_sleep(seconds: float, interval: float = 0.25) -> bool:
    deadline = time.time() + max(0.0, seconds)
    while time.time() < deadline:
        if not running:
            return False
        time.sleep(min(interval, max(0.0, deadline - time.time())))
    return running


signal.signal(signal.SIGINT, signal_handler)


def comment_key(name: str, text: str) -> str:
    normalized_name = re.sub(r"\s+", " ", (name or "")).strip().casefold()
    normalized_text = re.sub(r"\s+", " ", (text or "")).strip().casefold()
    return f"{normalized_name}|{normalized_text}"


def load_env_values(path: str) -> dict[str, str]:
    values = {}
    if not os.path.exists(path):
        return values

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue

            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]

            values[key] = value

    return values


def clean_comment_name(name: str) -> str:
    cleaned = (name or "").replace("\u00b7", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"^Comment by\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\s+\d+\s*(seconds?|minutes?|hours?|days?|weeks?|years?)\s*ago.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\s+about an?\s+(second|minute|hour|day|week|year).*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\s+\d+\s*[smhdwy]\s*$", "", cleaned, flags=re.IGNORECASE)
    if not cleaned or cleaned in SKIP_EXACT_LINES:
        return ""
    return cleaned.strip()


def is_noise_line(line: str, commenter: str = "") -> bool:
    if not line:
        return True
    if line == commenter:
        return True
    if line in SKIP_EXACT_LINES:
        return True
    return any(pattern.match(line) for pattern in SKIP_LINE_PATTERNS)


def normalize_comment_text(text: str, commenter: str) -> str:
    lines = []
    for raw_line in (text or "").splitlines():
        stripped = re.sub(r"\s+", " ", raw_line.replace("\u00b7", " ")).strip()
        if is_noise_line(stripped, commenter):
            continue
        if lines and lines[-1] == stripped:
            continue
        lines.append(stripped)

    body = "\n".join(lines).strip()
    if not body or body.upper() in {"GIPHY", "GIF"}:
        return ""
    if body == commenter:
        return ""
    if len(body) < 5 and not re.search(r"[\u1000-\u109F]", body):
        return ""
    return body


def click_matching_elements(page, patterns: list[str], limit: int = 1) -> int:
    try:
        clicked = page.evaluate(
            r"""(config) => {
                const regexes = config.patterns.map((pattern) => new RegExp(pattern, "i"));
                const isVisible = (el) => {
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return (
                        style &&
                        style.display !== "none" &&
                        style.visibility !== "hidden" &&
                        rect.width > 0 &&
                        rect.height > 0
                    );
                };

                const candidates = [];
                const seen = new Set();
                const nodes = document.querySelectorAll('div[role="button"], [role="button"], button, a, span');

                for (const node of nodes) {
                    const target = node.closest('div[role="button"], [role="button"], button, a') || node;
                    if (!target || seen.has(target) || !isVisible(target)) {
                        continue;
                    }

                    const text = (target.innerText || target.textContent || "").replace(/\s+/g, " ").trim();
                    if (!text || !regexes.some((regex) => regex.test(text))) {
                        continue;
                    }

                    seen.add(target);
                    candidates.push(target);
                }

                candidates.sort((left, right) => right.getBoundingClientRect().top - left.getBoundingClientRect().top);

                let clicked = 0;
                for (const target of candidates) {
                    if (clicked >= config.limit) {
                        break;
                    }

                    target.scrollIntoView({ behavior: "instant", block: "center" });
                    try {
                        target.click();
                    } catch (error) {
                        target.dispatchEvent(
                            new MouseEvent("click", { bubbles: true, cancelable: true, view: window })
                        );
                    }
                    clicked += 1;
                }

                return clicked;
            }""",
            {"patterns": patterns, "limit": limit},
        )
        return int(clicked or 0)
    except Exception:
        return 0


def scroll_comments_view(page):
    page.evaluate(
        r"""() => {
            const selectors = [
                '[aria-label*="Comment by"]',
                '[aria-label*="comment by"]',
                'div[data-ad-comet-preview="message"]',
                'div[data-ad-preview="message"]',
            ];

            let lastNode = null;
            for (const selector of selectors) {
                const nodes = document.querySelectorAll(selector);
                if (nodes.length > 0) {
                    lastNode = nodes[nodes.length - 1];
                }
            }

            if (lastNode) {
                lastNode.scrollIntoView({ behavior: "instant", block: "center" });
            }

            window.scrollBy(0, 1200);
            window.scrollTo(0, document.body.scrollHeight);

            document.querySelectorAll('[role="main"], [role="feed"], [data-pagelet*="Comment"]').forEach((node) => {
                node.scrollTop = node.scrollHeight;
            });
        }"""
    )


def switch_to_all_comments(page):
    print("  Switching to 'All comments'...")
    opened = click_matching_elements(page, [r"Most relevant", r"Top comments", r"Relevant"], limit=1)
    if opened:
        time.sleep(1.5)

    selected = click_matching_elements(page, [r"All comments"], limit=1)
    if selected:
        time.sleep(2.5)
        print("  Switched to All comments")
    else:
        print("  Warning: Could not switch filter (may already be on All)")


def expand_truncated_comments(page, limit: int = 20) -> int:
    return click_matching_elements(page, [r"^See more$", r"^See more replies?$"], limit=limit)


def extract_comments(page) -> list[dict]:
    """Extract comments from the rendered page using multiple Facebook DOM patterns."""
    try:
        raw_comments = page.evaluate(
            r"""() => {
                const normalize = (value) =>
                    (value || "")
                        .replace(/\u00b7/g, " ")
                        .replace(/\s+/g, " ")
                        .trim();

                const isVisible = (el) => {
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return (
                        style &&
                        style.display !== "none" &&
                        style.visibility !== "hidden" &&
                        rect.width > 0 &&
                        rect.height > 0
                    );
                };

                const isActionText = (text) =>
                    /^(Like|Reply|Share|Follow|Edited|Author|Top fan|Most relevant|All comments|Top comments|Newest|See more|See less|View more replies|View previous replies|See translation|Translated from|Write a (public )?reply.*|Write a (public )?comment.*|Send stars?)$/i.test(
                        text
                    );

                const parseNameFromLabel = (label) => {
                    let value = (label || "").trim();
                    value = value.replace(/^Comment by\s+/i, "");
                    value = value.replace(/\s+\d+\s*(seconds?|minutes?|hours?|days?|weeks?|years?)\s*ago.*$/i, "");
                    value = value.replace(/\s+about an?\s+(second|minute|hour|day|week|year).*$/i, "");
                    value = value.replace(/\s+\d+\s*[smhdwy]\s*$/i, "");
                    return value.trim();
                };

                const candidates = [];
                const seenRoots = new Set();

                const addCandidate = (root, bodyNode = null) => {
                    if (!root || seenRoots.has(root)) {
                        return;
                    }
                    seenRoots.add(root);
                    candidates.push({ root, bodyNode });
                };

                document
                    .querySelectorAll('div[data-ad-comet-preview="message"], div[data-ad-preview="message"]')
                    .forEach((bodyNode) => {
                        const root = bodyNode.closest(
                            '[aria-label*="Comment by"], [aria-label*="comment by"], div[role="article"], li'
                        );
                        addCandidate(root || bodyNode.parentElement, bodyNode);
                    });

                document
                    .querySelectorAll('[aria-label*="Comment by"], [aria-label*="comment by"], div[role="article"]')
                    .forEach((root) => {
                        const bodyNode = root.querySelector(
                            'div[data-ad-comet-preview="message"], div[data-ad-preview="message"]'
                        );
                        addCandidate(root, bodyNode);
                    });

                const results = [];
                for (const { root, bodyNode } of candidates) {
                    if (!root || !isVisible(root)) {
                        continue;
                    }

                    const rootText = normalize(root.innerText);
                    if (!rootText || /write a (public )?comment/i.test(rootText)) {
                        continue;
                    }

                    const rootLabel = root.getAttribute("aria-label") || "";
                    const hasCommentSignal =
                        /comment by/i.test(rootLabel) ||
                        /\bReply\b/i.test(rootText) ||
                        root.querySelector('a[href*="comment_id"], a[href*="reply_comment_id"]');

                    if (!hasCommentSignal) {
                        continue;
                    }

                    let name = parseNameFromLabel(rootLabel);
                    if (!name) {
                        for (const anchor of root.querySelectorAll("a[href]")) {
                            const text = normalize(anchor.textContent);
                            const href = anchor.getAttribute("href") || "";
                            if (
                                !text ||
                                isActionText(text) ||
                                /^\d+\s*[smhdwy]$/i.test(text) ||
                                /^\d+$/.test(text) ||
                                /^(View|See)\b/i.test(text)
                            ) {
                                continue;
                            }
                            if (
                                !href.includes("facebook.com") &&
                                !href.startsWith("/") &&
                                !href.includes("profile.php")
                            ) {
                                continue;
                            }
                            name = text;
                            break;
                        }
                    }

                    if (!name) {
                        continue;
                    }

                    const blocks = [];
                    const seenBlocks = new Set();
                    const addBlock = (value) => {
                        const text = normalize(value);
                        if (!text || seenBlocks.has(text) || isActionText(text)) {
                            return;
                        }
                        seenBlocks.add(text);
                        blocks.push(text);
                    };

                    if (bodyNode && isVisible(bodyNode)) {
                        addBlock(bodyNode.innerText || bodyNode.textContent || "");
                    }

                    if (!blocks.length) {
                        const leafNodes = Array.from(root.querySelectorAll('div[dir="auto"], span[dir="auto"]')).filter(
                            (node) => {
                                if (!isVisible(node)) {
                                    return false;
                                }
                                if (bodyNode && bodyNode.contains(node)) {
                                    return false;
                                }
                                if (node.closest('[role="button"], button, a')) {
                                    return false;
                                }
                                return !node.querySelector('div[dir="auto"], span[dir="auto"]');
                            }
                        );

                        leafNodes.forEach((node) => addBlock(node.innerText || node.textContent || ""));
                    }

                    if (!blocks.length) {
                        addBlock(rootText);
                    }

                    const body = blocks.join("\n").trim();
                    if (!body) {
                        continue;
                    }

                    results.push({ commenter: name, text: body });
                }

                return results;
            }"""
        )
    except Exception:
        return []

    comments = []
    seen_comment_keys = set()
    for raw_comment in raw_comments:
        commenter = clean_comment_name(raw_comment.get("commenter", ""))
        if not commenter:
            continue

        body = normalize_comment_text(raw_comment.get("text", ""), commenter)
        if not body:
            continue

        key = comment_key(commenter, body)
        if key in seen_comment_keys:
            continue
        seen_comment_keys.add(key)
        comments.append({"commenter": commenter, "text": body})

    return comments


def extract_post_text_keys(page) -> set[str]:
    try:
        raw_blocks = page.evaluate(
            r"""() => {
                const normalize = (value) =>
                    (value || "")
                        .replace(/\u00b7/g, " ")
                        .replace(/\s+/g, " ")
                        .trim();

                const isVisible = (el) => {
                    const style = window.getComputedStyle(el);
                    const rect = el.getBoundingClientRect();
                    return (
                        style &&
                        style.display !== "none" &&
                        style.visibility !== "hidden" &&
                        rect.width > 0 &&
                        rect.height > 0
                    );
                };

                const nodes = Array.from(
                    document.querySelectorAll(
                        'div[role="main"] div[data-ad-comet-preview="message"], ' +
                        'div[role="main"] div[data-ad-preview="message"], ' +
                        'div[role="feed"] div[data-ad-comet-preview="message"], ' +
                        'div[role="feed"] div[data-ad-preview="message"]'
                    )
                )
                    .filter((node) => {
                        if (!isVisible(node)) {
                            return false;
                        }
                        if (node.closest('[aria-label*="Comment by"], [aria-label*="comment by"]')) {
                            return false;
                        }
                        return true;
                    })
                    .sort((left, right) => left.getBoundingClientRect().top - right.getBoundingClientRect().top);

                const blocks = [];
                const seen = new Set();
                for (const node of nodes) {
                    const text = normalize(node.innerText || node.textContent || "");
                    if (!text || seen.has(text)) {
                        continue;
                    }
                    seen.add(text);
                    blocks.push(text);
                    if (blocks.length >= 3) {
                        break;
                    }
                }

                return blocks;
            }"""
        )
    except Exception:
        return set()

    post_keys = set()
    for block in raw_blocks:
        normalized = normalize_comment_text(block, "")
        if not normalized:
            normalized = re.sub(r"\s+", " ", (block or "")).strip()
        if len(normalized) < 40:
            continue
        post_keys.add(re.sub(r"\s+", " ", normalized).strip().casefold())
    return post_keys


def visible_comment_count(page) -> int:
    return len(extract_comments(page))


def load_more_comments(page, target: int = 100) -> int:
    """Scroll and click comment loaders until target new comments appear or no more are found."""
    loaded = 0
    stale_count = 0
    prev_count = visible_comment_count(page)
    print(f"    Starting with {prev_count} comments visible")

    while running and stale_count < 10:
        clicked = click_matching_elements(
            page,
            [
                r"View more comments",
                r"View previous comments",
                r"See more comments",
                r"View \d+ more comments?",
                r"View more replies",
                r"See more replies",
                r"View \d+ more repl",
                r"\d+ of \d+ comments?",
            ],
            limit=3,
        )

        if clicked:
            print(f"    Clicked {clicked} load-more control(s)")
            if not interruptible_sleep(random.uniform(1.5, 2.5)):
                break

        scroll_comments_view(page)
        if not interruptible_sleep(random.uniform(1.5, 2.5)):
            break

        expanded = expand_truncated_comments(page, limit=8)
        if expanded and not interruptible_sleep(0.75):
            break

        current_count = visible_comment_count(page)
        new_loaded = max(0, current_count - prev_count)

        if new_loaded > 0:
            loaded += new_loaded
            prev_count = current_count
            stale_count = 0
            print(f"    {current_count} comments visible so far...")
        else:
            stale_count += 1
            print(f"    No new comments yet (attempt {stale_count}/10)")

        if loaded >= target:
            break

    print(f"    Loaded {loaded} new comments this round")
    return loaded


def crawl_batch(page, post_url: str, seen_keys: set, post_text_keys: set, first_load: bool) -> list[dict]:
    """Load more comments and extract unseen ones."""
    if first_load:
        print("  Loading post...")
        page.goto(post_url, wait_until="domcontentloaded", timeout=60000)
        if not interruptible_sleep(random.uniform(5, 8)):
            return []

        if not post_text_keys:
            post_text_keys.update(extract_post_text_keys(page))

        print("  Scrolling to comments area...")
        for _ in range(5):
            if not running:
                return []
            scroll_comments_view(page)
            if not interruptible_sleep(random.uniform(1.0, 2.0)):
                return []

        switch_to_all_comments(page)

        try:
            page_title = page.title()
            if re.search(r"log\s*in|login", page_title, re.IGNORECASE):
                print("  Warning: Facebook redirected to login. Your cookies may be expired.")
        except Exception:
            pass

    print("  Loading more comments...")
    load_more_comments(page, target=BATCH_SIZE)
    if not running:
        return []

    print("  Expanding visible comments...")
    expanded = expand_truncated_comments(page, limit=40)
    if expanded:
        print(f"    Expanded {expanded} truncated comment(s)")
        if not interruptible_sleep(1):
            return []

    print("  Extracting comments...")
    raw_comments = extract_comments(page)
    if post_text_keys:
        raw_comments = [
            comment
            for comment in raw_comments
            if re.sub(r"\s+", " ", comment["text"]).strip().casefold() not in post_text_keys
        ]
    if not raw_comments:
        print("  Warning: No visible comments were detected on the page.")

    new_comments = []
    for comment in raw_comments:
        if not running:
            break
        key = comment_key(comment["commenter"], comment["text"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        new_comments.append(comment)
        if len(new_comments) >= BATCH_SIZE:
            break

    return new_comments


def wait_with_countdown(minutes: int):
    total = minutes * 60 + random.randint(-15, 15)
    total = max(60, total)
    print(f"\n  Next batch in about {minutes} minute(s)...")
    for remaining in range(total, 0, -1):
        if not running:
            return
        mins, secs = divmod(remaining, 60)
        print(f"\r  Next batch in {mins:02d}:{secs:02d}  ", end="", flush=True)
        if not interruptible_sleep(1):
            return
    print()


def main():
    print("=" * 50)
    print("  Facebook Comment Crawler (Playwright)")
    print(f"  {BATCH_SIZE} per batch, {WAIT_MINUTES}min interval")
    print("=" * 50)
    print("Press Ctrl+C to stop.\n")

    post_url = input("Facebook post URL:\n> ").strip()
    if not post_url:
        print("No URL. Exiting.")
        return

    env_values = load_env_values(ENV_PATH)
    c_user = env_values.get("c_user", "").strip()
    xs = env_values.get("xs", "").strip()

    if not c_user or not xs:
        print(f"Missing cookies in {ENV_PATH}.")
        print("Add c_user=... and xs=... to the .env file, then run again.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = os.path.join(OUTPUT_DIR, f"comments_{timestamp}.txt")

    with open(txt_path, "w", encoding="utf-8"):
        pass

    seen_keys = set()
    post_text_keys = set()
    all_saved = []
    batch_num = 0

    print(f"\nOutput: {txt_path}")
    print("Launching browser...\n")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )

        context.add_cookies(
            [
                {
                    "name": "c_user",
                    "value": c_user,
                    "domain": ".facebook.com",
                    "path": "/",
                },
                {
                    "name": "xs",
                    "value": xs,
                    "domain": ".facebook.com",
                    "path": "/",
                },
            ]
        )

        page = context.new_page()

        while running:
            batch_num += 1
            now = datetime.now().strftime("%H:%M:%S")
            print(f"-- Batch {batch_num} ({now}) --")

            first_load = batch_num == 1

            try:
                new_comments = crawl_batch(page, post_url, seen_keys, post_text_keys, first_load)
            except Exception as error:
                print(f"  Warning: {error}")
                print("  Retrying in 10 seconds...")
                if not interruptible_sleep(10):
                    break
                try:
                    new_comments = crawl_batch(
                        page,
                        post_url,
                        seen_keys,
                        post_text_keys,
                        first_load=True,
                    )
                except Exception as retry_error:
                    print(f"  Warning: Retry failed: {retry_error}")
                    new_comments = []

            if not new_comments:
                print("  No new comments found. All done!")
                break

            with open(txt_path, "a", encoding="utf-8") as output_file:
                for comment in new_comments:
                    all_saved.append(comment["text"])
                    output_file.write(f"[{len(all_saved)}]\n{comment['text']}\n\n")

            print(f"  {len(new_comments)} new comments (total: {len(all_saved)})")
            wait_with_countdown(WAIT_MINUTES)

        print(f"\n{'=' * 50}")
        print("Final summary:")
        print(f"   Batches:  {batch_num}")
        print(f"   Comments: {len(all_saved)}")
        print(f"   Output:   {txt_path}")
        print(f"{'=' * 50}")

        browser.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nForce stopped.")
