import fs from "fs/promises";
import path from "path";
import { chromium } from "playwright";

const SEARCH_URL =
  "https://search.danawa.com/dsearch.php?query=%EC%82%BC%EB%8B%A4%EC%88%98+2L&checkedInfo=N&volumeType=va&page=1&limit=120&sort=saveDESC&list=list&boost=true&tab=goods&addDelivery=N&coupangMemberSort=N&simpleDescOpen=Y&recommendedSort=N&defaultUICategoryCode=1623153&defaultPhysicsCategoryCode=46803%7C46819%7C56982%7C0&defaultVmTab=12&defaultVaTab=10406&isZeroPrice=Y&quickProductYN=N&priceUnitSort=Y&priceUnitSortOrder=A";

const OUTPUT_DIR = "data";
const OUTPUT_FILE = path.join(OUTPUT_DIR, "danawa-resolved.json");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeUrl(input) {
  try {
    return new URL(input).toString();
  } catch {
    return null;
  }
}

function normalizeUrl(raw) {
  const parsed = safeUrl(raw);
  if (!parsed) return null;

  try {
    const u = new URL(parsed);

    // 흔한 추적 파라미터 제거
    [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_term",
      "utm_content",
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
      "n_click"
    ].forEach((k) => u.searchParams.delete(k));

    // 해시 제거
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

async function getCanonicalUrl(page) {
  try {
    const href = await page.locator('link[rel="canonical"]').first().getAttribute("href", { timeout: 3000 });
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

    // 2초 이상 URL 변화 없으면 안정화로 간주
    if (stableCount >= 2) break;
  }

  return page.url();
}

async function resolveLink(browser, item) {
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
    keyword: item.keyword,
    title: item.title,
    mallName: item.mallName,
    price: item.price,
    danawaListUrl: item.danawaListUrl,
    danawaLinkHref: item.danawaLinkHref,
    bridgeUrl: item.bridgeUrl,
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
    const finalProductUrl = normalizeUrl(canonicalUrl || resolvedUrl);

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

async function scrapeDanawaList(page) {
  await page.goto(SEARCH_URL, {
    waitUntil: "domcontentloaded",
    timeout: 30000
  });

  await page.waitForLoadState("networkidle").catch(() => {});
  await sleep(2000);

  // 페이지 구조가 자주 바뀔 수 있으니 셀렉터를 넓게 잡음
  const items = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll("li[class*='product-item'], div[class*='prod_main_info'], li[class*='prod_item']"));
    const uniq = [];
    const seen = new Set();

    for (const row of rows) {
      const a =
        row.querySelector("a[href*='prod.danawa.com/bridge/']") ||
        row.querySelector("a[href*='loadingBridge']") ||
        row.querySelector("a[href*='prod.danawa.com']");

      if (!a) continue;

      const href = a.getAttribute("href");
      if (!href || seen.has(href)) continue;
      seen.add(href);

      const text = row.textContent || "";

      // 광고/추천성 블록 최대한 배제
      if (text.includes("이런 상품 어때요")) continue;

      const titleEl =
        row.querySelector("a[title]") ||
        row.querySelector("p[class*='prod_name'] a") ||
        row.querySelector("div[class*='title'] a") ||
        a;

      const priceEl =
        row.querySelector("[class*='price_sect'] strong") ||
        row.querySelector("[class*='num']") ||
        row.querySelector("strong");

      const mallEl =
        row.querySelector("[class*='mall']") ||
        row.querySelector("[class*='seller']") ||
        row.querySelector("[class*='shop']");

      uniq.push({
        title: (titleEl?.textContent || "").trim(),
        price: (priceEl?.textContent || "").replace(/[^\d]/g, ""),
        mallName: (mallEl?.textContent || "").trim(),
        danawaLinkHref: href
      });
    }

    return uniq;
  });

  return items.map((item) => ({
    keyword: "삼다수 2L",
    title: item.title,
    mallName: item.mallName,
    price: item.price,
    danawaListUrl: SEARCH_URL,
    danawaLinkHref: item.danawaLinkHref,
    bridgeUrl: item.danawaLinkHref.startsWith("http")
      ? item.danawaLinkHref
      : new URL(item.danawaLinkHref, "https://search.danawa.com").toString()
  }));
}

async function main() {
  await fs.mkdir(OUTPUT_DIR, { recursive: true });

  const browser = await chromium.launch({
    headless: true
  });

  try {
    const page = await browser.newPage({
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    });

    const list = await scrapeDanawaList(page);

    // 너무 많으면 액션 시간 길어지므로 우선 상위 20개만
    const targets = list.slice(0, 20);

    const results = [];
    for (let i = 0; i < targets.length; i += 1) {
      const item = targets[i];
      console.log(`[${i + 1}/${targets.length}] resolving: ${item.title}`);

      const resolved = await resolveLink(browser, item);
      results.push(resolved);

      // 과도한 연속 접근 완화
      await sleep(1500);
    }

    await fs.writeFile(
      OUTPUT_FILE,
      JSON.stringify(
        {
          collectedAt: new Date().toISOString(),
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
    await browser.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});