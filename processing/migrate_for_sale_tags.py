"""
RETIRED — migration 13 deprecates the For Sale system tag.

For Sale presence is now read directly from the External Layer `layer_for_sale`
in reference.db. The taggings rows written by earlier runs of this script
remain in the database as a historical archive with system=1; application code
no longer reads them.

Do not run this script. It is retained only to document the prior approach.
"""

import sys


def run():
    print(
        "migrate_for_sale_tags is retired.\n"
        "For Sale presence is now an External Layer (layer_for_sale in reference.db).\n"
        "System tag rows from prior runs are archived in taggings (system=1) but not read."
    )
    sys.exit(0)


if __name__ == "__main__":
    run()
