"""
RETIRED — migration 13 deprecates GIS layer presence system tags.

GIS layer presence is now read directly from the Derived Layer `parcels_gis`
(columns in reference.db). The taggings rows written by earlier runs of this
script remain in the database as a historical archive with system=1;
application code no longer reads them.

Do not run this script. It is retained only to document the prior approach.
"""

import sys


def run():
    print(
        "migrate_gis_tags is retired.\n"
        "GIS layer presence is now a Derived Layer (parcels_gis columns in reference.db).\n"
        "System tag rows from prior runs are archived in taggings (system=1) but not read."
    )
    sys.exit(0)


if __name__ == "__main__":
    run()
