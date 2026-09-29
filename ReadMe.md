### How to run bot automatically on Ubuntu server.

0 8 * * * /home/vaia-bot/run_cron.sh

### Hotel crawl settings

Requires the `hotel_list.crawl_scope` column and
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
