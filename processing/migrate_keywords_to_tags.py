"""
RETIRED — migration 13 deprecates OCR keyword system tags.

OCR keyword scores are now read directly from the External Layer
`DocumentArticle97KeywordScore` (registry_ocr.kw_* columns in reference.db).
The taggings rows written by earlier runs of this script remain in the database
as a historical archive with system=1; application code no longer reads them.

Do not run this script. It is retained only to document the prior approach.
"""

import sys


def run():
    print(
        "migrate_keywords_to_tags is retired.\n"
        "OCR keyword scores are now an External Layer (registry_ocr.kw_* in reference.db).\n"
        "System tag rows from prior runs are archived in taggings (system=1) but not read."
    )
    sys.exit(0)


if __name__ == "__main__":
    run()
