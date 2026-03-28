import fs from "fs/promises";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const RAW_FILE = "data/danawa-resolved.json";
const INSERT_BATCH_SIZE = 1000;

function toIntPrice(v) {
  const n = String(v || "").replace(/[^\d]/g, "");
  return n ? Number(n) : null;
}

async function main() {
  const raw = await fs.readFile(RAW_FILE, "utf8");
  const json = JSON.parse(raw);

  const crawlBatchId =
    json.collectedAt?.replace(/[:.]/g, "-") || new Date().toISOString().replace(/[:.]/g, "-");

  const rows = (json.results || []).map((item) => {
    const price = toIntPrice(item.price);
    const shippingFee =
      item.shipping_fee === 0 || item.shipping_fee
        ? Number(item.shipping_fee)
        : null;

    return {
      source: "danawa",
      keyword: item.keyword || null,
      raw_title: item.title || null,
      mall_name: item.mallName || null,
      price,
      delivery: item.delivery || null,
      shipping_fee: shippingFee,
      total_price:
        price != null && shippingFee != null ? price + shippingFee : null,
      final_product_url: item.finalProductUrl || null,
      danawa_list_url: item.danawaListUrl || null,
      danawa_link_href: item.danawaLinkHref || null,
      resolved_url: item.resolvedUrl || null,
      redirect_chain: item.redirectChain || null,
      raw_payload: item,
      collected_at: json.collectedAt || new Date().toISOString(),
      crawl_batch_id: crawlBatchId
    };
  });

  const validRows = rows.filter((r) => r.keyword && r.raw_title);

  if (!validRows.length) {
    console.log("No valid rows to insert.");
    return;
  }

  for (let i = 0; i < validRows.length; i += INSERT_BATCH_SIZE) {
    const chunk = validRows.slice(i, i + INSERT_BATCH_SIZE);
    const { error } = await supabase.from("raw_prices").insert(chunk);
    if (error) {
      throw error;
    }
  }

  console.log(`Inserted ${validRows.length} rows into raw_prices`);

  console.log("Running refresh_service_prices()...");
  const { error: refreshError } = await supabase.rpc("refresh_service_prices", {
    p_crawl_batch_id: crawlBatchId
  });

  if (refreshError) {
    throw refreshError;
  }

  console.log("refresh_service_prices() executed successfully");

}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
