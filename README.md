# tesla_modes_s

Late events beyond allowed lateness → drop or dead-letter (must be explicit) - just drop. No dead-letter yet.

A `page_view` can be emitted when you are confident no other `ad_click` will arrive that should supersede attribution (based on watermark), o you can emit updates to already produced `attributed_page_view` (but then specify your update strategy clearly). - watermark added
