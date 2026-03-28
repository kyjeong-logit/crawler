import fs from "fs/promises";
import path from "path";
import crypto from "crypto";
import { chromium } from "playwright";

const KEYWORDS = [
  "삼다수 2L",
  "삼다수 500ml",
  "삼다수 330ml",
  "삼다수 1l",
  "가야워터 2L",
  "가야워터 500ml",
  "백산수 330ml",
  "백산수 2L",
  "백산수 500ml",
  "백산수 1l",
  "아이시스 300ml",
  "아이시스 2L",
  "아이시스 500ml",
  "아이시스 1l",
];

const START_PAGE = 1;
const END_PAGE = 1;

// 액션 시간/차단 방지용
const MAX_ITEMS_PER_PAGE = 50;
const SLEEP_BETWEEN_LIST_PAGES_MS = 1200;
const SLEEP_BETWEEN_RESOLVE_MS = 1200;
const RESOLVE_CONCURRENCY = 3;

const OUTPUT_DIR = "data";
const OUTPUT_FILE = path.join(OUTPUT_DIR, "danawa-resolved.json");

const COUPANG_ACCESS_KEY = process.env.COUPANG_ACCESS_KEY || "";
const COUPANG_SECRET_KEY = process.env.COUPANG_SECRET_KEY || "";

const coupangDeeplinkCache = new Map();

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

function parseShippingFee(deliveryText) {
  const text = cleanText(deliveryText);

  if (!text) return null;
  if (text.includes("무료배송")) return 0;
  if (text.includes("배송비 무료")) return 0;

  const match = text.match(/배송비\s*([0-9][0-9,]*)\s*원/);
  if (match?.[1]) {
    return Number(match[1].replace(/[^\d]/g, ""));
  }

  return null;
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
      "inflow",
      "src",
      "spec",
      "addtag",
      "ctag",
      "lptag",
      "itime",
      "pageType",
      "pageValue",
      "wPcid",
      "wRef",
      "wTime",
      "redirect",
      "mcid",
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
      "return_url",
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

function isCoupangProductUrl(url) {
  try {
    const u = new URL(url);
    return /(^|\.)coupang\.com$/i.test(u.hostname || "");
  } catch {
    return false;
  }
}

function buildCoupangAuthHeader(method, pathWithQuery, accessKey, secretKey) {
  const now = new Date();

  const yy = String(now.getUTCFullYear()).slice(2);
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  const hh = String(now.getUTCHours()).padStart(2, "0");
  const mi = String(now.getUTCMinutes()).padStart(2, "0");
  const ss = String(now.getUTCSeconds()).padStart(2, "0");

  const signedDate = `${yy}${mm}${dd}T${hh}${mi}${ss}Z`;

  const [path, query = ""] = pathWithQuery.split("?");
  const message = `${signedDate}${method}${path}${query}`;

  const signature = crypto
    .createHmac("sha256", secretKey)
    .update(message, "utf8")
    .digest("hex");

  return `CEA algorithm=HmacSHA256, access-key=${accessKey}, signed-date=${signedDate}, signature=${signature}`;
}

function pickCoupangShortUrl(payload) {
  const candidates = [
    payload?.data,
    payload?.rData,
    payload?.resultData,
    payload?.results,
  ];

  for (const arr of candidates) {
    if (!Array.isArray(arr) || arr.length === 0) continue;

    for (const row of arr) {
      const shortUrl =
        row?.shortenUrl ||
        row?.shortURL ||
        row?.shortUrl ||
        row?.short_link ||
        row?.shortLink ||
        row?.deeplink;

      if (typeof shortUrl === "string" && shortUrl.startsWith("http")) {
        return shortUrl;
      }
    }
  }

  return null;
}

async function createCoupangDeeplink(originalUrl) {
  if (!originalUrl) return originalUrl;
  if (!isCoupangProductUrl(originalUrl)) return originalUrl;

  if (coupangDeeplinkCache.has(originalUrl)) {
    return coupangDeeplinkCache.get(originalUrl);
  }

  if (!COUPANG_ACCESS_KEY || !COUPANG_SECRET_KEY) {
    console.warn("[COUPANG] access/secret key 없음. 원본 URL 유지");
    coupangDeeplinkCache.set(originalUrl, originalUrl);
    return originalUrl;
  }

  const endpointCandidates = [
    "/v2/providers/affiliate_open_api/apis/openapi/v1/deeplink",
    "/v2/providers/affiliate_open_api/apis/openapi/deeplink",
  ];

  for (const apiPath of endpointCandidates) {
    try {
      const authHeader = buildCoupangAuthHeader(
        "POST",
        apiPath,
        COUPANG_ACCESS_KEY,
        COUPANG_SECRET_KEY
      );

      const res = await fetch(`https://api-gateway.coupang.com${apiPath}`, {
        method: "POST",
        headers: {
          Authorization: authHeader,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          coupangUrls: [originalUrl],
        }),
      });

      if (res.status === 404) {
        continue;
      }

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`status=${res.status} body=${text}`);
      }

      const data = await res.json();
      const deepLink = pickCoupangShortUrl(data);

      if (deepLink) {
        coupangDeeplinkCache.set(originalUrl, deepLink);
        return deepLink;
      }

      console.warn("[COUPANG] 응답 성공했으나 short url 없음:", JSON.stringify(data));
    } catch (err) {
      console.warn(`[COUPANG] Deeplink 변환 실패 (${apiPath})`, err);
    }
  }

  coupangDeeplinkCache.set(originalUrl, originalUrl);
  return originalUrl;
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

  console.log(`[DEBUG] open list url -> ${danawaListUrl}`);

  await page.goto(danawaListUrl, {
    waitUntil: "domcontentloaded",
    timeout: 30000,
  });

  await page.waitForLoadState("networkidle").catch(() => {});
  await sleep(2500);

  const payload = await page.evaluate(
    ({ keyword, pageNo, danawaListUrl, maxItemsPerPage }) => {
      function cleanText(v) {
        return String(v || "")
          .replace(/\u00a0/g, " ")
          .replace(/\s+/g, " ")
          .trim();
      }

      function isBadSectionText(text) {
        const t = cleanText(text);
        return (
          !t ||
          t.includes("오늘 봐야 할 추천 상품") ||
          t.includes("이런 상품 어때요") ||
          t.includes("추천상품") ||
          t.includes("광고")
        );
      }

      function isAdOrRecommendRow(row) {
        if (!row) return true;

        const cls = String(row.className || "").toLowerCase();
        const rowText = cleanText(row.textContent || "");

        if (cls.includes("ad")) return true;
        if (cls.includes("advert")) return true;
        if (cls.includes("recommend")) return true;
        if (cls.includes("power")) return true;

        if (rowText.includes("오늘 봐야 할 추천 상품")) return true;
        if (rowText.includes("이런 상품 어때요")) return true;
        if (rowText.includes("추천상품")) return true;

        return false;
      }

      function pickProductTitle(row) {
        const selectors = [
          ".prod_info .prod_name a",
          "p.prod_name a",
          ".prod_name a",
          "a[name='productName']",
        ];

        for (const selector of selectors) {
          const el = row.querySelector(selector);
          if (!el) continue;

          const text = cleanText(el.textContent || el.innerText || "");
          if (!text) continue;
          if (text.length < 3) continue;
          if (/\d[\d,]*원/.test(text)) continue;
          if (/무료배송|배송비|가격비교|관심상품|상품분류/.test(text)) continue;

          return text;
        }

        return "";
      }

      function pickProductLink(row) {
        const selectors = [
          ".prod_info .prod_name a[href]",
          "p.prod_name a[href]",
          ".prod_name a[href]",
          "a[name='productName'][href]",
        ];

        for (const selector of selectors) {
          const el = row.querySelector(selector);
          if (!el) continue;
          const href = el.href || el.getAttribute("href") || "";
          if (href) return href;
        }

        return "";
      }

      function pickMallAnchor(row) {
        const selectors = [
          "div.price_info li a.click_log_product_searched_price_",
          "ul.prod_pricelist li a.click_log_product_searched_price_",
          ".price_info li a",
          ".prod_pricelist li a",
        ];

        for (const selector of selectors) {
          const anchors = Array.from(row.querySelectorAll(selector));
          for (const a of anchors) {
            const txt = cleanText(a.textContent || "");
            if (!txt) continue;
            return a;
          }
        }

        return null;
      }

      function pickMallName(row, mallAnchor) {
        const img =
          mallAnchor?.querySelector(".mall_icon img[alt]") ||
          mallAnchor?.querySelector("p.mall_icon img[alt]");

        const alt = cleanText(img?.getAttribute("alt") || "");
        if (
          alt &&
          !/\d[\d,]*원/.test(alt) &&
          !/무료배송|배송비|가격비교|관심상품|상품분류/.test(alt)
        ) {
          return alt;
        }

        const fallbackText = cleanText(
          mallAnchor?.querySelector(".mall_icon")?.textContent || ""
        );
        if (
          fallbackText &&
          !/\d[\d,]*원/.test(fallbackText) &&
          !/무료배송|배송비|가격비교|관심상품|상품분류/.test(fallbackText)
        ) {
          return fallbackText;
        }

        return "";
      }

      function pickMallLinkHref(mallAnchor) {
        if (!mallAnchor) return "";
        return mallAnchor.href || mallAnchor.getAttribute("href") || "";
      }

      function pickPrice(row, mallAnchor) {
        const texts = [];

        const strong = cleanText(
          mallAnchor?.querySelector(".price_sect strong")?.textContent || ""
        );
        if (strong) texts.push(strong);

        const priceSect = cleanText(
          mallAnchor?.querySelector(".price_sect")?.textContent || ""
        );
        if (priceSect) texts.push(priceSect);

        const rowText = cleanText(row.textContent || "");
        if (rowText) texts.push(rowText);

        for (const text of texts) {
          const m = text.match(/([0-9][0-9,]{2,})\s*원/);
          if (m && m[1]) {
            return m[1].replace(/[^\d]/g, "");
          }
        }

        for (const text of texts) {
          const arr = text.match(/[0-9][0-9,]{2,}/g) || [];
          for (const v of arr) {
            const num = v.replace(/[^\d]/g, "");
            if (!num) continue;
            if (Number(num) < 1000) continue;
            return num;
          }
        }

        return "";
      }

      function pickDelivery(row, mallAnchor) {
        const shipText = cleanText(
          mallAnchor?.querySelector(".ship_sect")?.textContent || ""
        );
        if (shipText) {
          if (shipText.includes("무료배송")) return "무료배송";
          if (shipText.includes("배송비")) return shipText;
        }

        const rowText = cleanText(row.textContent || "");
        if (rowText.includes("무료배송")) return "무료배송";

        const m = rowText.match(/배송비\s*[^\s]+/);
        if (m) return cleanText(m[0]);

        return "";
      }

      function parseShippingFee(deliveryText) {
        const text = cleanText(deliveryText);

        if (!text) return null;
        if (text.includes("무료배송")) return 0;
        if (text.includes("배송비 무료")) return 0;

        const match = text.match(/배송비\s*([0-9][0-9,]*)\s*원/);
        if (match?.[1]) {
          return Number(match[1].replace(/[^\d]/g, ""));
        }

        return null;
      }

      function pickRegDate(row) {
        const text = cleanText(row.textContent || "");
        const m1 = text.match(/\d{2}\.\d{2}\.\s*등록/);
        if (m1) return cleanText(m1[0]);

        const m2 = text.match(/\d{2}\.\d{2}\./);
        if (m2) return `${cleanText(m2[0])} 등록`;

        return "";
      }

      const rows = Array.from(
        document.querySelectorAll("li.prod_item, li[class*='prod_item']")
      );

      const out = [];
      const seen = new Set();
      const debugSamples = [];

      for (const row of rows) {
        if (!row) continue;
        if (isAdOrRecommendRow(row)) continue;

        const rowText = cleanText(row.textContent || "");
        if (!rowText) continue;
        if (isBadSectionText(rowText)) continue;

        const title = pickProductTitle(row);
        const productLinkHref = pickProductLink(row);
        const mallAnchor = pickMallAnchor(row);
        const danawaLinkHref = pickMallLinkHref(mallAnchor) || productLinkHref;
        const mallName = pickMallName(row, mallAnchor);
        const price = pickPrice(row, mallAnchor);
        const regDate = pickRegDate(row);
        const delivery = pickDelivery(row, mallAnchor);
        const shipping_fee = parseShippingFee(delivery);

        if (debugSamples.length < 5) {
          debugSamples.push({
            title,
            danawaLinkHref,
            price,
            mallName,
            regDate,
            delivery,
            shipping_fee,
            rowText: rowText.slice(0, 220),
          });
        }

        if (!title) continue;
        if (!danawaLinkHref) continue;
        if (!price) continue;

        const dedupeKey = `${title}|||${price}|||${danawaLinkHref}`;
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);

        out.push({
          keyword,
          page: pageNo,
          rank: out.length + 1,
          title,
          regDate,
          mallName,
          price,
          delivery,
          shipping_fee,
          danawaListUrl,
          danawaLinkHref,
        });

        if (out.length >= maxItemsPerPage) break;
      }

      return {
        rowCount: rows.length,
        extractedCount: out.length,
        debugSamples,
        items: out,
      };
    },
    { keyword, pageNo, danawaListUrl, maxItemsPerPage: MAX_ITEMS_PER_PAGE }
  );

  console.log(
    `[DEBUG] ${keyword} / page ${pageNo} -> rows=${payload.rowCount}, extracted=${payload.extractedCount}`
  );

  if (payload.debugSamples?.length) {
    console.log(
      `[DEBUG SAMPLES] ${keyword} / page ${pageNo}\n${JSON.stringify(payload.debugSamples, null, 2)}`
    );
  }

  return payload.items.map((item) => ({
    ...item,
    bridgeUrl: item.danawaLinkHref.startsWith("http")
      ? item.danawaLinkHref
      : new URL(item.danawaLinkHref, "https://search.danawa.com").toString(),
  }));
}

async function resolveItem(browser, item) {
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    viewport: { width: 1440, height: 1200 },
    javaScriptEnabled: true,
    ignoreHTTPSErrors: true,
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
    error: null,
  };

  try {
    await page.goto(item.bridgeUrl, {
      waitUntil: "domcontentloaded",
      timeout: 20000,
    });

    await waitForFinalUrl(page, 15000);

    const resolvedUrl = page.url();
    const canonicalUrl = await getCanonicalUrl(page);

    let finalProductUrl = resolveFinalProductUrl({
      canonicalUrl,
      resolvedUrl,
      redirectChain: hops.slice(),
    });

    if (finalProductUrl && isCoupangProductUrl(finalProductUrl)) {
      finalProductUrl = await createCoupangDeeplink(finalProductUrl);
    }

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
    headless: true,
  });

  const listContext = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    viewport: { width: 1440, height: 1200 },
    javaScriptEnabled: true,
    ignoreHTTPSErrors: true,
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

    const results = new Array(listRows.length);
    let nextIndex = 0;

    async function worker(workerId) {
      while (true) {
        const currentIndex = nextIndex;
        nextIndex += 1;

        if (currentIndex >= listRows.length) break;

        const item = listRows[currentIndex];

        console.log(
          `[W${workerId}] [RESOLVE ${currentIndex + 1}/${listRows.length}] ${item.keyword} / p${item.page} / #${item.rank} / ${item.title}`
        );

        try {
          const resolved = await resolveItem(browser, item);
          results[currentIndex] = resolved;
        } catch (err) {
          results[currentIndex] = {
            ...item,
            decodedSLink: extractSLink(item.bridgeUrl),
            resolvedUrl: null,
            canonicalUrl: null,
            finalProductUrl: null,
            redirectChain: [],
            error: err instanceof Error ? err.message : String(err),
          };
        }

        await sleep(SLEEP_BETWEEN_RESOLVE_MS);
      }
    }

    await Promise.all(
      Array.from({ length: RESOLVE_CONCURRENCY }, (_, i) => worker(i + 1))
    );

    const finalResults = results.filter(Boolean);

    await fs.writeFile(
      OUTPUT_FILE,
      JSON.stringify(
        {
          collectedAt: new Date().toISOString(),
          keywords: KEYWORDS,
          startPage: START_PAGE,
          endPage: END_PAGE,
          maxItemsPerPage: MAX_ITEMS_PER_PAGE,
          resolveConcurrency: RESOLVE_CONCURRENCY,
          count: finalResults.length,
          results: finalResults,
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
