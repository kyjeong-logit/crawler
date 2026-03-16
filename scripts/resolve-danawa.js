import fs from "fs/promises";
import path from "path";
import { chromium } from "playwright";

const KEYWORDS = [
  "삼다수 2L",
  "삼다수 500ml"
];

const START_PAGE = 1;
const END_PAGE = 2;

// 액션 시간/차단 방지용
const MAX_ITEMS_PER_PAGE = 30;
const SLEEP_BETWEEN_LIST_PAGES_MS = 1200;
const SLEEP_BETWEEN_RESOLVE_MS = 1200;

const OUTPUT_DIR = "data";
const OUTPUT_FILE = path.join(OUTPUT_DIR, "danawa-resolved.json");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function cleanText(v) {
  return String(v || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function safeUrl(input) {
  try {
    return new URL(input).toString();
  } catch {
    return null;
  }
}

function decodeMaybe(v) {
  try {
    return decodeURIComponent(v);
  } catch {
    return v;
  }
}

function buildSearchUrl(keyword, page) {
  const url = new URL("https://search.danawa.com/dsearch.php");

  url.searchParams.set("query", keyword);
  url.searchParams.set("checkedInfo", "N");
  url.searchParams.set("volumeType", "va");
  url.searchParams.set("page", String(page));
  url.searchParams.set("limit", "120");
  url.searchParams.set("sort", "saveDESC");
  url.searchParams.set("list", "list");
  url.searchParams.set("boost", "true");
  url.searchParams.set("tab", "goods");
  url.searchParams.set("addDelivery", "N");
  url.searchParams.set("coupangMemberSort", "N");
  url.searchParams.set("simpleDescOpen", "Y");
  url.searchParams.set("recommendedSort", "N");
  url.searchParams.set("defaultUICategoryCode", "1623153");
  url.searchParams.set("defaultPhysicsCategoryCode", "46803|46819|56982|0");
  url.searchParams.set("defaultVmTab", "12");
  url.searchParams.set("defaultVaTab", "10406");
  url.searchParams.set("isZeroPrice", "Y");
  url.searchParams.set("quickProductYN", "N");
  url.searchParams.set("priceUnitSort", "Y");
  url.searchParams.set("priceUnitSortOrder", "A");

  return url.toString();
}

function normalizeUrl(raw) {
  const parsed = safeUrl(raw);
  if (!parsed) return null;

  try {
    const u = new URL(parsed);

    [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_term",
      "utm_content",
      "gclid",
      "fbclid",
      "msclkid",
      "mc_cid",
      "mc_eid",
      "NaPm",
      "n_media",
      "n_query",
      "n_rank",
      "n_ad_group",
      "n_ad",
      "n_campaign",
      "n_mall_id",
      "n_mall_pid",
      "n_place",
      "n_mat",
      "n_click",
      "n_keyword",
      "n_gate",
      "n_oc",
      "campaignUuid",
      "service",
      "from",
      "tr",
      "inflow"
    ].forEach((k) => u.searchParams.delete(k));

    u.hash = "";
    return u.toString();
  } catch {
    return parsed;
  }
}

function extractSLink(urlString) {
  try {
    const u = new URL(urlString);
    const sLink = u.searchParams.get("sLink");
    if (!sLink) return null;

    const decoded = Buffer.from(sLink, "base64").toString("utf8");
    return safeUrl(decoded) || decoded;
  } catch {
    return null;
  }
}

function unwrapTrackingUrl(rawUrl) {
  if (!rawUrl) return null;

  let current = rawUrl;
  let changed = true;
  let guard = 0;

  while (changed && guard < 10) {
    guard += 1;
    changed = false;

    const parsed = safeUrl(current);
    if (!parsed) break;

    const u = new URL(parsed);
    const host = (u.hostname || "").toLowerCase();

    const paramKeys = [
      "url",
      "u",
      "target",
      "dest",
      "destination",
      "redirect",
      "redirect_url",
      "redir",
      "rurl",
      "goUrl",
      "dl",
      "returnUrl",
      "return_url"
    ];

    for (const key of paramKeys) {
      const value = u.searchParams.get(key);
      if (!value) continue;
      const decoded = decodeMaybe(value);
      if (/^https?:\/\//i.test(decoded)) {
        current = decoded;
        changed = true;
        break;
      }
    }
    if (changed) continue;

    if (host.includes("google.")) {
      const q = u.searchParams.get("q") || u.searchParams.get("url");
      if (q) {
        const decoded = decodeMaybe(q);
        if (/^https?:\/\//i.test(decoded)) {
          current = decoded;
          changed = true;
          continue;
        }
      }
    }

    if (host.includes("11pcs.11st.co.kr")) {
      const goUrl = u.searchParams.get("goUrl");
      if (goUrl) {
        const decoded = decodeMaybe(goUrl);
        if (/^https?:\/\//i.test(decoded)) {
          current = decoded;
          changed = true;
          continue;
        }
      }
    }
  }

  return safeUrl(current) || current;
}

function isTrackingLikeUrl(rawUrl) {
  const parsed = safeUrl(rawUrl);
  if (!parsed) return true;

  try {
    const u = new URL(parsed);
    const host = (u.hostname || "").toLowerCase();
    const path = (u.pathname || "").toLowerCase();

    if (host.includes("google.") && (path === "/url" || path.includes("/aclk"))) return true;
    if (host.includes("11pcs.11st.co.kr")) return true;
    if (host.includes("bridge.danawa.com")) return true;
    if (host.includes("prod.danawa.com") && path.includes("/bridge/")) return true;
    if (host.includes("click.linkprice.com")) return true;
    if (host.includes("tracking.")) return true;
    if (host.includes("tracker.")) return true;
    if (host.includes("adcr.naver.com")) return true;

    return false;
  } catch {
    return true;
  }
}

function normalizeUsefulUrl(rawUrl) {
  const unwrapped = unwrapTrackingUrl(rawUrl);
  if (!unwrapped) return null;
  return normalizeUrl(unwrapped);
}

function pickLastUsefulUrl(redirectChain) {
  if (!Array.isArray(redirectChain) || redirectChain.length === 0) return null;

  const cleaned = redirectChain
    .filter((v) => typeof v === "string" && !v.startsWith("HTTP "))
    .map((v) => normalizeUsefulUrl(v))
    .filter(Boolean)
    .filter((v) => !isTrackingLikeUrl(v));

  if (cleaned.length === 0) return null;
  return cleaned[cleaned.length - 1];
}

function resolveFinalProductUrl({ canonicalUrl, resolvedUrl, redirectChain }) {
  const canonical = normalizeUsefulUrl(canonicalUrl);
  if (canonical) return canonical;

  const lastUseful = pickLastUsefulUrl(redirectChain);
  if (lastUseful) return lastUseful;

  const resolved = normalizeUsefulUrl(resolvedUrl);
  if (resolved) return resolved;

  return null;
}

async function getCanonicalUrl(page) {
  try {
    const href = await page
      .locator('link[rel="canonical"]')
      .first()
      .getAttribute("href", { timeout: 3000 });

    if (!href) return null;
    return safeUrl(href) || href;
  } catch {
    return null;
  }
}

async function waitForFinalUrl(page, timeoutMs = 15000) {
  const started = Date.now();
  let lastUrl = page.url();
  let stableCount = 0;

  while (Date.now() - started < timeoutMs) {
    await sleep(1000);
    const current = page.url();

    if (current === lastUrl) {
      stableCount += 1;
    } else {
      stableCount = 0;
      lastUrl = current;
    }

    if (stableCount >= 2) break;
  }

  return page.url();
}

async function scrapeDanawaListPage(page, { keyword, pageNo }) {
  const danawaListUrl = buildSearchUrl(keyword, pageNo);

  await page.goto(danawaListUrl, {
    waitUntil: "domcontentloaded",
    timeout: 30000
  });

  await page.waitForLoadState("networkidle").catch(() => {});
  await sleep(1500);

  const items = await page.evaluate(({ keyword, pageNo, danawaListUrl, maxItemsPerPage }) => {
    function cleanText(v) {
      return String(v || "")
        .replace(/\u00a0/g, " ")
        .replace(/\s+/g, " ")
        .trim();
    }

    function pickFirstText(root, selectors, options = {}) {
      const {
        minLength = 1,
        rejectValues = [],
        rejectIncludes = []
      } = options;

      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          const text = cleanText(node.textContent || node.innerText || "");
          if (!text) continue;
          if (text.length < minLength) continue;
          if (rejectValues.includes(text)) continue;
          if (rejectIncludes.some((x) => text.includes(x))) continue;
          return text;
        }
      }
      return "";
    }

    function pickHref(root, selectors) {
      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          const href = node.href || node.getAttribute("href") || "";
          if (href) return href;
        }
      }
      return "";
    }

    function pickPrice(root) {
      const selectors = [
        ".price_sect .price_tier .num",
        ".prod_pricelist .price_sect .num",
        ".main_price .num",
        ".price .num",
        "[class*='price'] .num",
        "strong[class*='price']",
        ".num"
      ];

      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          const text = cleanText(node.textContent || "");
          const price = text.replace(/[^\d]/g, "");
          if (price) return price;
        }
      }
      return "";
    }

    function pickRegDate(root) {
      const wholeText = cleanText(root.textContent || "");

      const m = wholeText.match(/\d{2}\.\d{2}\.\s*등록/);
      if (m) return cleanText(m[0]);

      const m2 = wholeText.match(/\d{2}\.\d{2}\./);
      if (m2) return `${cleanText(m2[0])} 등록`;

      return "";
    }

    function pickDelivery(root) {
      const candidates = [];

      const selectors = [
        ".price_sect .stxt",
        ".price_sect .delivery_info",
        ".delivery_info",
        ".delivery",
        "[class*='delivery']",
        "[class*='ship']"
      ];

      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          const text = cleanText(node.textContent || "");
          if (!text) continue;
          candidates.push(text);
        }
      }

      for (const text of candidates) {
        if (text.includes("무료배송")) return "무료배송";
      }

      for (const text of candidates) {
        if (text.includes("배송비")) return text;
      }

      return "";
    }

    const rows = Array.from(
      document.querySelectorAll(
        ".prod_list > ul > li.prod_item, .main_prodlist > ul > li.prod_item, li.prod_item"
      )
    );

    const out = [];
    const seen = new Set();
    let rank = 0;

    for (const row of rows) {
      const rowText = cleanText(row.textContent || "");
      if (!rowText) continue;

      if (rowText.includes("이런 상품 어때요")) continue;
      if (row.classList.contains("advertising")) continue;
      if (row.classList.contains("ad_item")) continue;
      if (row.classList.contains("product-pot")) continue;

      const danawaLinkHref = pickHref(row, [
        ".prod_info .prod_name a[href]",
        "p.prod_name a[href]",
        ".prod_name a[href]",
        "a[name='productName'][href]",
        "a[href*='prod.danawa.com/bridge/']",
        "a[href*='loadingBridge']",
        "a[href*='prod.danawa.com']"
      ]);

      if (!danawaLinkHref) continue;
      if (seen.has(danawaLinkHref)) continue;
      seen.add(danawaLinkHref);

      const title = pickFirstText(
        row,
        [
          ".prod_info .prod_name a",
          "p.prod_name a",
          ".prod_name a",
          "a[name='productName']",
          "a[title]"
        ],
        {
          minLength: 3,
          rejectValues: ["관심", "찜", "비교", "보기", "등록", "장바구니"],
          rejectIncludes: ["관심상품", "비교하기", "장바구니"]
        }
      );

      const mallName = pickFirstText(
        row,
        [
          ".price_sect .mall_name",
          ".mall_name",
          ".seller_name",
          ".seller",
          ".shop_name",
          "[class*='mall_name']",
          "[class*='seller']",
          "[class*='shop']"
        ],
        {
          minLength: 2,
          rejectValues: ["관심", "찜", "비교", "구매"],
          rejectIncludes: ["장바구니", "비교하기", "구매하기"]
        }
      );

      const price = pickPrice(row);
      const regDate = pickRegDate(row);
      const delivery = pickDelivery(row);

      if (!title || !price) continue;

      rank += 1;

      out.push({
        keyword,
        page: pageNo,
        rank,
        title,
        regDate,
        mallName,
        price,
        delivery,
        danawaListUrl,
        danawaLinkHref
      });

      if (out.length >= maxItemsPerPage) break;
    }

    return out;
  }, { keyword, pageNo, danawaListUrl, maxItemsPerPage: MAX_ITEMS_PER_PAGE });

  return items.map((item) => ({
    ...item,
    bridgeUrl: item.danawaLinkHref.startsWith("http")
      ? item.danawaLinkHref
      : new URL(item.danawaLinkHref, "https://search.danawa.com").toString()
  }));
}

async function resolveItem(browser, item) {
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    viewport: { width: 1440, height: 1200 },
    javaScriptEnabled: true,
    ignoreHTTPSErrors: true
  });

  const page = await context.newPage();
  const hops = [];
  const maxHops = 20;

  page.on("framenavigated", (frame) => {
    if (frame !== page.mainFrame()) return;
    const url = frame.url();
    if (!url) return;
    if (hops[hops.length - 1] !== url) {
      hops.push(url);
      if (hops.length > maxHops) hops.shift();
    }
  });

  page.on("response", (response) => {
    try {
      const status = response.status();
      if (status >= 300 && status < 400) {
        const location = response.headers()["location"];
        if (location) {
          hops.push(`HTTP ${status} -> ${location}`);
          if (hops.length > maxHops) hops.shift();
        }
      }
    } catch {}
  });

  const result = {
    ...item,
    decodedSLink: extractSLink(item.bridgeUrl),
    resolvedUrl: null,
    canonicalUrl: null,
    finalProductUrl: null,
    redirectChain: [],
    error: null
  };

  try {
    await page.goto(item.bridgeUrl, {
      waitUntil: "domcontentloaded",
      timeout: 20000
    });

    await waitForFinalUrl(page, 15000);

    const resolvedUrl = page.url();
    const canonicalUrl = await getCanonicalUrl(page);
    const finalProductUrl = resolveFinalProductUrl({
      canonicalUrl,
      resolvedUrl,
      redirectChain: hops.slice()
    });

    result.resolvedUrl = resolvedUrl;
    result.canonicalUrl = canonicalUrl;
    result.finalProductUrl = finalProductUrl;
    result.redirectChain = hops.slice();
  } catch (err) {
    result.error = err instanceof Error ? err.message : String(err);
    result.redirectChain = hops.slice();
  } finally {
    await context.close();
  }

  return result;
}

async function main() {
  await fs.mkdir(OUTPUT_DIR, { recursive: true });

  const browser = await chromium.launch({
    headless: true
  });

  const listContext = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    viewport: { width: 1440, height: 1200 },
    javaScriptEnabled: true,
    ignoreHTTPSErrors: true
  });

  const listPage = await listContext.newPage();

  try {
    const listRows = [];

    for (const keyword of KEYWORDS) {
      for (let pageNo = START_PAGE; pageNo <= END_PAGE; pageNo += 1) {
        console.log(`[LIST] ${keyword} / ${pageNo} page`);

        const rows = await scrapeDanawaListPage(listPage, { keyword, pageNo });
        listRows.push(...rows);

        console.log(`[LIST] ${keyword} / ${pageNo} page -> ${rows.length}건`);
        await sleep(SLEEP_BETWEEN_LIST_PAGES_MS);
      }
    }

    console.log(`[LIST] total rows: ${listRows.length}`);

    const results = [];
    for (let i = 0; i < listRows.length; i += 1) {
      const item = listRows[i];
      console.log(
        `[RESOLVE ${i + 1}/${listRows.length}] ${item.keyword} / p${item.page} / #${item.rank} / ${item.title}`
      );

      const resolved = await resolveItem(browser, item);
      results.push(resolved);

      await sleep(SLEEP_BETWEEN_RESOLVE_MS);
    }

    await fs.writeFile(
      OUTPUT_FILE,
      JSON.stringify(
        {
          collectedAt: new Date().toISOString(),
          keywords: KEYWORDS,
          startPage: START_PAGE,
          endPage: END_PAGE,
          maxItemsPerPage: MAX_ITEMS_PER_PAGE,
          count: results.length,
          results
        },
        null,
        2
      ),
      "utf8"
    );

    console.log(`saved -> ${OUTPUT_FILE}`);
  } finally {
    await listContext.close();
    await browser.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});