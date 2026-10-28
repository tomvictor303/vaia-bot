### How to run the bot automatically on an Ubuntu server

0 8 * * * /home/vaia-bot/run_cron.sh

Run the command daily. By default, the application processes only hotels due
that day according to `hotel_list.crawl_cycle`:

```sh
node index.js
```

Force all eligible hotels while ignoring their crawl cycles:

```sh
node index.js --all
```

Force one eligible hotel while ignoring its crawl cycle:

```sh
node index.js --hotel <hotel_uuid>
```

All modes require `is_market_data_capture_enabled = 1` and `is_deleted = 0`.

Supported crawl cycles and their default run dates:

| `crawl_cycle` | Default run date |
| --- | --- |
| `daily` | Every day |
| `weekly` | Sunday |
| `every_2_weeks` | Every second Sunday, anchored to January 4, 1970 |
| `monthly` | First day of every month |
| `every_2_months` | First day of January, March, May, July, September, and November |

The default database value is `monthly`. Cycle evaluation uses the server's
local calendar date. Crawl offsets are not currently applied.

### Hotel crawl settings

Requires the `hotel_list.crawl_scope` and `hotel_list.crawl_cycle` columns and
`hotel_additional_urls` table (`id`, `hotel_id`, `url`, `is_enabled`).

- `site` (default): follow pages on the main URL's host.
- `subtree`: follow the main path and its descendants on that host.
- `page`: scrape only the main page.

Enabled rows in `hotel_additional_urls` (`hotel_id`, `url`, `is_enabled`)
are exact-page exceptions for all modes (`site`, `subtree`, and `page`).
Only `hotel_url` starts the crawl. An exception is followed when discovered in a
link, subject to the existing depth and file-type limits; it is not seeded independently.
Links from exception pages must still satisfy the main hotel's scope or be explicitly listed.
Queries are retained; fragments are ignored. HTTP/HTTPS and trailing slashes
are treated as equivalent when checking scope. Additional URLs must use the same
hostname and port as `hotel_url`. Use the canonical hostname in
`hotel_url`; redirects outside the scope are skipped unless the destination is
an explicit exception. They do not expand the main scope.

If no pages are saved, existing pages are not deactivated. Existing HTML cleanup
and LLM processing are unchanged; changing scope does not remove old facts from
previously aggregated market data.

Run URL-scope tests without a database or LLM:

```sh
node --test tests/crawlScope.test.js
```

Run crawl-cycle tests:

```sh
node --test tests/crawlSchedule.test.js
```
