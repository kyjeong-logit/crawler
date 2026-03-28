-- =============================================================================
-- Supabase → SQL Editor 에 전체 복붙 후 Run 한 번 실행
--
-- 전제:
--   - 뷰 public.vw_raw_prices_parsed 에 raw_prices.id 와 같은 bigint 컬럼 id 가 있음
--   - public.url_encode(text) 함수가 이미 있음 (없으면 STEP 3에서 오류)
--   - products 에 ON CONFLICT (category, brand_name, volume_ml, pack_count) 용 유니크
--
-- 앱/스크립트: upload-to-supabase.js 는 RPC 에 p_crawl_batch_id 를 넘기는 버전이어야 함
-- 수동 전체 갱신: select public.refresh_service_prices(null);
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_raw_prices_crawl_batch_id
  ON public.raw_prices (crawl_batch_id);

CREATE INDEX IF NOT EXISTS idx_offers_dedup_lookup
  ON public.offers (product_id, store_id, collected_at, (coalesce(final_url, '')));

DROP FUNCTION IF EXISTS public.refresh_service_prices();
DROP FUNCTION IF EXISTS public.refresh_service_prices(text);

CREATE FUNCTION public.refresh_service_prices(p_crawl_batch_id text DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = public
AS $$
BEGIN
  DROP TABLE IF EXISTS tmp_refresh_parsed;

  CREATE TEMPORARY TABLE tmp_refresh_parsed
  ON COMMIT DROP
  AS
  SELECT v.*
  FROM public.vw_raw_prices_parsed v
  INNER JOIN public.raw_prices r ON r.id = v.id
  WHERE p_crawl_batch_id IS NULL
     OR r.crawl_batch_id = p_crawl_batch_id;

  IF NOT EXISTS (SELECT 1 FROM tmp_refresh_parsed LIMIT 1) THEN
    RETURN;
  END IF;

  INSERT INTO public.stores (
    store_name,
    store_type,
    supports_affiliate,
    affiliate_provider,
    is_active,
    created_at,
    updated_at
  )
  SELECT DISTINCT
    trim(v.mall_name) AS store_name,
    'open_market' AS store_type,
    coalesce(s0.supports_affiliate, false) AS supports_affiliate,
    s0.affiliate_provider,
    true AS is_active,
    now() AS created_at,
    now() AS updated_at
  FROM tmp_refresh_parsed v
  LEFT JOIN public.stores s0
    ON s0.store_name = trim(v.mall_name)
  WHERE coalesce(trim(v.mall_name), '') <> ''
  ON CONFLICT (store_name) DO UPDATE
  SET
    updated_at = now(),
    is_active = true;

  INSERT INTO public.products (
    category,
    brand_name,
    product_name,
    volume_ml,
    pack_count,
    unit_base,
    search_keyword,
    is_active,
    created_at,
    updated_at
  )
  SELECT
    'water' AS category,
    x.parsed_brand_name AS brand_name,
    concat(
      x.parsed_brand_name, ' ',
      x.parsed_volume_ml::text, 'ml ',
      x.parsed_pack_count::text, '입'
    ) AS product_name,
    x.parsed_volume_ml AS volume_ml,
    x.parsed_pack_count AS pack_count,
    '100ml' AS unit_base,
    min(x.norm_keyword) AS search_keyword,
    true AS is_active,
    now() AS created_at,
    now() AS updated_at
  FROM tmp_refresh_parsed x
  WHERE x.parsed_brand_name IS NOT NULL
    AND x.parsed_volume_ml IS NOT NULL
    AND x.parsed_pack_count IS NOT NULL
  GROUP BY
    x.parsed_brand_name,
    x.parsed_volume_ml,
    x.parsed_pack_count
  ON CONFLICT (category, brand_name, volume_ml, pack_count) DO UPDATE
  SET
    updated_at = now(),
    search_keyword = coalesce(public.products.search_keyword, excluded.search_keyword),
    is_active = true;

  DROP TABLE IF EXISTS tmp_offer_refresh;

  CREATE TEMPORARY TABLE tmp_offer_refresh
  ON COMMIT DROP
  AS
  WITH src AS (
    SELECT
      v.id AS raw_price_id,
      p.id AS product_id,
      s.id AS store_id,

      p.brand_name,
      p.volume_ml,
      p.pack_count,
      p.total_volume_ml,

      s.store_name AS seller_name,
      v.norm_title AS title,
      v.resolved_url AS product_url,

      CASE
        WHEN s.supports_affiliate = true
         AND s.affiliate_provider = 'linkprice'
         AND coalesce(s.affiliate_merchant_code, '') <> ''
         AND coalesce(s.affiliate_a, '') <> ''
         AND coalesce(s.deeplink_supported, false) = true
         AND coalesce(v.resolved_url, '') <> ''
        THEN
          CASE
            WHEN s.affiliate_merchant_code IN ('gmarket', 'auction', '11st', 'emart')
            THEN
              'https://lase.kr/click.php'
              || '?m=' || s.affiliate_merchant_code
              || '&a=' || s.affiliate_a
              || '&l=' || coalesce(s.affiliate_l, '9999')
              || '&l_cd1=' || coalesce(s.affiliate_l_cd1, '3')
              || '&l_cd2=' || coalesce(s.affiliate_l_cd2, '0')
              || '&tu=' || public.url_encode(v.resolved_url)

            WHEN s.affiliate_merchant_code = 'woori'
            THEN
              'https://newtip.net/click.php'
              || '?m=' || s.affiliate_merchant_code
              || '&a=' || s.affiliate_a
              || '&l=' || coalesce(s.affiliate_l, '9999')
              || '&l_cd1=' || coalesce(s.affiliate_l_cd1, '3')
              || '&l_cd2=' || coalesce(s.affiliate_l_cd2, '0')
              || '&tu=' || public.url_encode(v.resolved_url)

            ELSE v.resolved_url
          END

        ELSE v.resolved_url
      END AS final_url,

      v.price,

      CASE
        WHEN v.shipping_fee IS NOT NULL THEN v.shipping_fee
        WHEN coalesce(v.delivery, '') ILIKE '%무료배송%' THEN 0
        WHEN coalesce(v.delivery, '') ILIKE '%배송비 무료%' THEN 0
        ELSE NULL
      END AS shipping_fee_resolved,

      coalesce(v.collected_at, now()) AS collected_at
    FROM tmp_refresh_parsed v
    JOIN public.stores s
      ON s.store_name = trim(v.mall_name)
    JOIN public.products p
      ON p.brand_name = v.parsed_brand_name
     AND p.volume_ml = v.parsed_volume_ml
     AND p.pack_count = v.parsed_pack_count
    WHERE v.parsed_brand_name IS NOT NULL
      AND v.parsed_volume_ml IS NOT NULL
      AND v.parsed_pack_count IS NOT NULL
      AND coalesce(trim(v.mall_name), '') <> ''
      AND v.price IS NOT NULL
      AND coalesce(v.resolved_url, '') <> ''
      AND lower(v.resolved_url) NOT LIKE '%prod.danawa.com/bridge/%'
      AND lower(v.resolved_url) NOT LIKE '%gmarket.co.kr/n/search%'
      AND lower(v.resolved_url) NOT LIKE '%smartstore.naver.com/inflow/%'
      AND lower(v.resolved_url) NOT LIKE '%/click.php%'
      AND lower(v.resolved_url) NOT LIKE '%newtip.net/%'
      AND lower(v.resolved_url) NOT LIKE '%lase.kr/%'
  ),
  calc AS (
    SELECT
      s.*,
      coalesce(s.shipping_fee_resolved, 0) AS shipping_fee_final,
      s.price + coalesce(s.shipping_fee_resolved, 0) AS total_price_resolved,
      CASE
        WHEN coalesce(s.shipping_fee_resolved, 0) = 0 THEN true
        ELSE false
      END AS is_free_shipping
    FROM src s
  )
  SELECT
    c.raw_price_id,
    c.product_id,
    c.store_id,

    c.brand_name,
    c.volume_ml,
    c.pack_count,
    c.total_volume_ml,

    c.seller_name,
    c.title,
    c.product_url,
    c.final_url,
    c.price,
    c.shipping_fee_resolved,
    c.shipping_fee_final,
    c.total_price_resolved,
    c.is_free_shipping,

    CASE
      WHEN c.total_volume_ml IS NOT NULL
       AND c.total_volume_ml > 0
      THEN round(
        (c.total_price_resolved::numeric / c.total_volume_ml::numeric) * 100,
        2
      )
      ELSE NULL
    END AS price_per_100ml,

    CASE
      WHEN c.pack_count IS NOT NULL
       AND c.pack_count > 0
      THEN round(
        c.total_price_resolved::numeric / c.pack_count::numeric,
        2
      )
      ELSE NULL
    END AS price_per_each,

    true AS is_available,
    c.collected_at
  FROM calc c;

  DROP TABLE IF EXISTS tmp_offer_refresh_dedup;

  CREATE TEMPORARY TABLE tmp_offer_refresh_dedup
  ON COMMIT DROP
  AS
  SELECT *
  FROM (
    SELECT
      t.*,
      row_number() OVER (
        PARTITION BY
          t.product_id,
          t.store_id,
          coalesce(t.final_url, ''),
          t.collected_at
        ORDER BY t.raw_price_id DESC
      ) AS rn
    FROM tmp_offer_refresh t
  ) x
  WHERE x.rn = 1;

  UPDATE public.offers o
  SET
    product_id = t.product_id,
    store_id = t.store_id,

    brand_name = t.brand_name,
    volume_ml = t.volume_ml,
    pack_count = t.pack_count,
    total_volume_ml = t.total_volume_ml,

    seller_name = t.seller_name,
    title = t.title,
    product_url = t.product_url,
    final_url = t.final_url,
    price = t.price,
    shipping_fee = t.shipping_fee_final,
    is_free_shipping = t.is_free_shipping,
    price_per_100ml = t.price_per_100ml,
    price_per_each = t.price_per_each,
    is_available = t.is_available,
    collected_at = t.collected_at,
    updated_at = now()
  FROM tmp_offer_refresh_dedup t
  WHERE o.raw_price_id = t.raw_price_id
    AND NOT EXISTS (
      SELECT 1
      FROM public.offers o2
      WHERE o2.product_id = t.product_id
        AND o2.store_id = t.store_id
        AND coalesce(o2.final_url, '') = coalesce(t.final_url, '')
        AND o2.collected_at = t.collected_at
        AND o2.raw_price_id <> o.raw_price_id
    );

  INSERT INTO public.offers (
    product_id,
    store_id,
    raw_price_id,

    brand_name,
    volume_ml,
    pack_count,
    total_volume_ml,

    seller_name,
    title,
    product_url,
    final_url,
    price,
    shipping_fee,
    is_free_shipping,
    price_per_100ml,
    price_per_each,
    is_available,
    collected_at,
    created_at,
    updated_at
  )
  SELECT
    d.product_id,
    d.store_id,
    d.raw_price_id,

    d.brand_name,
    d.volume_ml,
    d.pack_count,
    d.total_volume_ml,

    d.seller_name,
    d.title,
    d.product_url,
    d.final_url,
    d.price,
    d.shipping_fee_final,
    d.is_free_shipping,
    d.price_per_100ml,
    d.price_per_each,
    d.is_available,
    d.collected_at,
    now(),
    now()
  FROM tmp_offer_refresh_dedup d
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.offers o
    WHERE o.raw_price_id = d.raw_price_id
  )
    AND NOT EXISTS (
      SELECT 1
      FROM public.offers o
      WHERE o.product_id = d.product_id
        AND o.store_id = d.store_id
        AND coalesce(o.final_url, '') = coalesce(d.final_url, '')
        AND o.collected_at = d.collected_at
    );
END;
$$;

COMMENT ON FUNCTION public.refresh_service_prices(text) IS
  'Refreshes stores/products/offers. Pass crawl_batch_id for incremental run after insert; NULL = full.';

-- GitHub Actions / 서버는 service_role 키로 호출. 클라이언트에서 RPC 쓰면 authenticated 등 추가.
GRANT EXECUTE ON FUNCTION public.refresh_service_prices(text) TO service_role;
