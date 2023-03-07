CREATE
OR REPLACE VIEW "public"."v_transactions_dgt_internal" AS
SELECT
     int_tab.date,
     int_tab.clinic_name,
     int_tab.country,
     int_tab."region",
     int_tab.no_of_transactions,
     int_tab.total_payment,
     int_tab.total_refund_amount,
     int_tab.net_amount,
     last_1w_transactions.no_of_transactions_last7,
     last_1w_transactions.net_amount_last7,
     last_4w_transactions.no_of_transactions_last30,
     last_4w_transactions.net_amount_last30
FROM
     (
          SELECT
               DISTINCT a.date,
               a.clinic_name,
               a.country,
               a."region",
               a.no_of_transactions,
               a.total_payment * (
                    1:: numeric / COALESCE(
                         exchange_rate_monthly.conv_rate,
                         "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                              PARTITION BY a.clinic_name
                              ORDER BY
                                   a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                   AND CURRENT ROW
                         )
                    )
               ) AS total_payment,
               a.total_refund_amount * (
                    1:: numeric / COALESCE(
                         exchange_rate_monthly.conv_rate,
                         "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                              PARTITION BY a.clinic_name
                              ORDER BY
                                   a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                   AND CURRENT ROW
                         )
                    )
               ) AS total_refund_amount,
               a.net_amount * (
                    1:: numeric / COALESCE(
                         exchange_rate_monthly.conv_rate,
                         "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                              PARTITION BY a.clinic_name
                              ORDER BY
                                   a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                   AND CURRENT ROW
                         )
                    )
               ) AS net_amount
          FROM
               (
                    SELECT
                         DISTINCT base.date,
                         base.id,
                         base.clinic_name,
                         base.country,
                         base."region",
                         base.no_of_transactions,
                         base.total_payment,
                         base.total_refund_amount,
                         base.net_amount,
                         clinic_currencies.value
                    FROM
                         (
                              SELECT
                                   date(charges.date) AS date,
                                   clinics.id,
                                   clinics.name AS clinic_name,
                                   countries.name AS country,
                                   CASE
                                   WHEN countries.name:: text = 'United States':: text
                                   OR countries.name:: text = 'Canada':: text THEN 'North America':: text
                                   WHEN countries.name:: text = 'South Africa':: text THEN 'South Africa':: text
                                   WHEN countries.name:: text = 'Romania':: text THEN 'Romania':: text
                                   ELSE 'Rest of the World':: text END AS "region",
                                   count(DISTINCT charges.id) AS no_of_transactions,
                                   sum(charges.amount) AS total_payment,
                                   sum(charges.amount_refunded) AS total_refund_amount,
                                   sum(charges.amount) - sum(charges.amount_refunded) AS net_amount
                              FROM
                                   dgt_live_schema.charges
                                   JOIN dgt_live_schema.sale ON charges.sale_id = sale.id
                                   JOIN dgt_live_schema.clinics ON sale.clinic_id = clinics.id
                                   JOIN dgt_live_schema.countries ON clinics.country_id = countries.id
                              WHERE
                                   sale.consolidated_by IS NULL
                                   AND sale.deleted_at IS NULL
                                   AND charges.deleted_at IS NULL
                              GROUP BY
                                   date(charges.date),
                                   clinics.id,
                                   clinics.name,
                                   countries.name,
                                   CASE
                                   WHEN countries.name:: text = 'United States':: text
                                   OR countries.name:: text = 'Canada':: text THEN 'North America':: text
                                   WHEN countries.name:: text = 'South Africa':: text THEN 'South Africa':: text
                                   WHEN countries.name:: text = 'Romania':: text THEN 'Romania':: text
                                   ELSE 'Rest of the World':: text END
                         ) base
                         JOIN (
                              SELECT
                                   clinics.id,
                                   clinics.name,
                                   object_preferences.value
                              FROM
                                   dgt_live_schema.object_preferences
                                   JOIN dgt_live_schema.clinics ON object_preferences.object_id = clinics.country_id
                              WHERE
                                   object_preferences.object_type:: text ~ ~ '%Country':: text
                                   AND object_preferences.preference_id = 66
                         ) clinic_currencies ON base.id = clinic_currencies.id
               ) a
               LEFT JOIN exchange_rate_monthly ON date_trunc('month':: text, a.date:: timestamp without time zone) = exchange_rate_monthly.date:: timestamp without time zone
               AND a.value:: text = exchange_rate_monthly.currency:: text
     ) int_tab
     LEFT JOIN (
          SELECT
               int_tab.clinic_name,
               int_tab.country,
               int_tab."region",
               min(int_tab.date) AS min,
               "max"(int_tab.date) AS "max",
               sum(int_tab.no_of_transactions) AS no_of_transactions_last7,
               sum(int_tab.net_amount) AS net_amount_last7
          FROM
               (
                    SELECT
                         DISTINCT a.date,
                         a.clinic_name,
                         a.country,
                         a."region",
                         a.no_of_transactions,
                         a.total_payment * (
                              1:: numeric / COALESCE(
                                   exchange_rate_monthly.conv_rate,
                                   "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                                        PARTITION BY a.clinic_name
                                        ORDER BY
                                             a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                             AND CURRENT ROW
                                   )
                              )
                         ) AS total_payment,
                         a.total_refund_amount * (
                              1:: numeric / COALESCE(
                                   exchange_rate_monthly.conv_rate,
                                   "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                                        PARTITION BY a.clinic_name
                                        ORDER BY
                                             a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                             AND CURRENT ROW
                                   )
                              )
                         ) AS total_refund_amount,
                         a.net_amount * (
                              1:: numeric / COALESCE(
                                   exchange_rate_monthly.conv_rate,
                                   "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                                        PARTITION BY a.clinic_name
                                        ORDER BY
                                             a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                             AND CURRENT ROW
                                   )
                              )
                         ) AS net_amount
                    FROM
                         (
                              SELECT
                                   DISTINCT base.date,
                                   base.id,
                                   base.clinic_name,
                                   base.country,
                                   base."region",
                                   base.no_of_transactions,
                                   base.total_payment,
                                   base.total_refund_amount,
                                   base.net_amount,
                                   clinic_currencies.value
                              FROM
                                   (
                                        SELECT
                                             date(charges.date) AS date,
                                             clinics.id,
                                             clinics.name AS clinic_name,
                                             countries.name AS country,
                                             CASE
                                             WHEN countries.name:: text = 'United States':: text
                                             OR countries.name:: text = 'Canada':: text THEN 'North America':: text
                                             WHEN countries.name:: text = 'South Africa':: text THEN 'South Africa':: text
                                             WHEN countries.name:: text = 'Romania':: text THEN 'Romania':: text
                                             ELSE 'Rest of the World':: text END AS "region",
                                             count(DISTINCT charges.id) AS no_of_transactions,
                                             sum(charges.amount) AS total_payment,
                                             sum(charges.amount_refunded) AS total_refund_amount,
                                             sum(charges.amount) - sum(charges.amount_refunded) AS net_amount
                                        FROM
                                             dgt_live_schema.charges
                                             JOIN dgt_live_schema.sale ON charges.sale_id = sale.id
                                             JOIN dgt_live_schema.clinics ON sale.clinic_id = clinics.id
                                             JOIN dgt_live_schema.countries ON clinics.country_id = countries.id
                                        WHERE
                                             sale.consolidated_by IS NULL
                                             AND sale.deleted_at IS NULL
                                             AND charges.deleted_at IS NULL
                                        GROUP BY
                                             date(charges.date),
                                             clinics.id,
                                             clinics.name,
                                             countries.name,
                                             CASE
                                             WHEN countries.name:: text = 'United States':: text
                                             OR countries.name:: text = 'Canada':: text THEN 'North America':: text
                                             WHEN countries.name:: text = 'South Africa':: text THEN 'South Africa':: text
                                             WHEN countries.name:: text = 'Romania':: text THEN 'Romania':: text
                                             ELSE 'Rest of the World':: text END
                                   ) base
                                   JOIN (
                                        SELECT
                                             clinics.id,
                                             clinics.name,
                                             object_preferences.value
                                        FROM
                                             dgt_live_schema.object_preferences
                                             JOIN dgt_live_schema.clinics ON object_preferences.object_id = clinics.country_id
                                        WHERE
                                             object_preferences.object_type:: text ~ ~ '%Country':: text
                                             AND object_preferences.preference_id = 66
                                   ) clinic_currencies ON base.id = clinic_currencies.id
                         ) a
                         LEFT JOIN exchange_rate_monthly ON date_trunc('month':: text, a.date:: timestamp without time zone) = exchange_rate_monthly.date:: timestamp without time zone
                         AND a.value:: text = exchange_rate_monthly.currency:: text
               ) int_tab
          WHERE
               date_diff(
                    'day':: text,
                    int_tab.date:: timestamp without time zone,
                    'now':: text:: date:: timestamp without time zone
               ) >= 1
               AND date_diff(
                    'day':: text,
                    int_tab.date:: timestamp without time zone,
                    'now':: text:: date:: timestamp without time zone
               ) <= 7
          GROUP BY
               int_tab.clinic_name,
               int_tab.country,
               int_tab."region"
     ) last_1w_transactions ON int_tab.clinic_name:: text = last_1w_transactions.clinic_name:: text
     AND int_tab.country:: text = last_1w_transactions.country:: text
     LEFT JOIN (
          SELECT
               int_tab.clinic_name,
               int_tab.country,
               int_tab."region",
               min(int_tab.date) AS min,
               "max"(int_tab.date) AS "max",
               sum(int_tab.no_of_transactions) AS no_of_transactions_last30,
               sum(int_tab.net_amount) AS net_amount_last30
          FROM
               (
                    SELECT
                         DISTINCT a.date,
                         a.clinic_name,
                         a.country,
                         a."region",
                         a.no_of_transactions,
                         a.total_payment * (
                              1:: numeric / COALESCE(
                                   exchange_rate_monthly.conv_rate,
                                   "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                                        PARTITION BY a.clinic_name
                                        ORDER BY
                                             a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                             AND CURRENT ROW
                                   )
                              )
                         ) AS total_payment,
                         a.total_refund_amount * (
                              1:: numeric / COALESCE(
                                   exchange_rate_monthly.conv_rate,
                                   "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                                        PARTITION BY a.clinic_name
                                        ORDER BY
                                             a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                             AND CURRENT ROW
                                   )
                              )
                         ) AS total_refund_amount,
                         a.net_amount * (
                              1:: numeric / COALESCE(
                                   exchange_rate_monthly.conv_rate,
                                   "last_value"(exchange_rate_monthly.conv_rate IGNORE NULLS) OVER(
                                        PARTITION BY a.clinic_name
                                        ORDER BY
                                             a.date ROWS BETWEEN UNBOUNDED PRECEDING
                                             AND CURRENT ROW
                                   )
                              )
                         ) AS net_amount
                    FROM
                         (
                              SELECT
                                   DISTINCT base.date,
                                   base.id,
                                   base.clinic_name,
                                   base.country,
                                   base."region",
                                   base.no_of_transactions,
                                   base.total_payment,
                                   base.total_refund_amount,
                                   base.net_amount,
                                   clinic_currencies.value
                              FROM
                                   (
                                        SELECT
                                             date(charges.date) AS date,
                                             clinics.id,
                                             clinics.name AS clinic_name,
                                             countries.name AS country,
                                             CASE
                                             WHEN countries.name:: text = 'United States':: text
                                             OR countries.name:: text = 'Canada':: text THEN 'North America':: text
                                             WHEN countries.name:: text = 'South Africa':: text THEN 'South Africa':: text
                                             WHEN countries.name:: text = 'Romania':: text THEN 'Romania':: text
                                             ELSE 'Rest of the World':: text END AS "region",
                                             count(DISTINCT charges.id) AS no_of_transactions,
                                             sum(charges.amount) AS total_payment,
                                             sum(charges.amount_refunded) AS total_refund_amount,
                                             sum(charges.amount) - sum(charges.amount_refunded) AS net_amount
                                        FROM
                                             dgt_live_schema.charges
                                             JOIN dgt_live_schema.sale ON charges.sale_id = sale.id
                                             JOIN dgt_live_schema.clinics ON sale.clinic_id = clinics.id
                                             JOIN dgt_live_schema.countries ON clinics.country_id = countries.id
                                        WHERE
                                             sale.consolidated_by IS NULL
                                             AND sale.deleted_at IS NULL
                                             AND charges.deleted_at IS NULL
                                        GROUP BY
                                             date(charges.date),
                                             clinics.id,
                                             clinics.name,
                                             countries.name,
                                             CASE
                                             WHEN countries.name:: text = 'United States':: text
                                             OR countries.name:: text = 'Canada':: text THEN 'North America':: text
                                             WHEN countries.name:: text = 'South Africa':: text THEN 'South Africa':: text
                                             WHEN countries.name:: text = 'Romania':: text THEN 'Romania':: text
                                             ELSE 'Rest of the World':: text END
                                   ) base
                                   JOIN (
                                        SELECT
                                             clinics.id,
                                             clinics.name,
                                             object_preferences.value
                                        FROM
                                             dgt_live_schema.object_preferences
                                             JOIN dgt_live_schema.clinics ON object_preferences.object_id = clinics.country_id
                                        WHERE
                                             object_preferences.object_type:: text ~ ~ '%Country':: text
                                             AND object_preferences.preference_id = 66
                                   ) clinic_currencies ON base.id = clinic_currencies.id
                         ) a
                         LEFT JOIN exchange_rate_monthly ON date_trunc('month':: text, a.date:: timestamp without time zone) = exchange_rate_monthly.date:: timestamp without time zone
                         AND a.value:: text = exchange_rate_monthly.currency:: text
               ) int_tab
          WHERE
               date_diff(
                    'day':: text,
                    int_tab.date:: timestamp without time zone,
                    'now':: text:: date:: timestamp without time zone
               ) >= 1
               AND date_diff(
                    'day':: text,
                    int_tab.date:: timestamp without time zone,
                    'now':: text:: date:: timestamp without time zone
               ) <= 30
          GROUP BY
               int_tab.clinic_name,
               int_tab.country,
               int_tab."region"
     ) last_4w_transactions ON int_tab.clinic_name:: text = last_4w_transactions.clinic_name:: text
     AND int_tab.country:: text = last_4w_transactions.country:: text;
