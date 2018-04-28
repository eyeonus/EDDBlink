# EDDBlink
A plugin for TradeDangerous to update market data using EDDB's api files.

This plugin is released under the LGPLv3, a copy of the licence is included in the repo.

To use, place "eddblink_plug.py" into your TradeDangerous 'plugin' directory and run with '-P eddblink'

The plugin needs to make some changes to the database in order to work, so on first run, it will run with the '-O clean' option enabled regardless of the options passed to it from the command line. '-O skipvend' will skip updating the ShipVendor and UpgradeVendor tables regardless of other options, including on the first run.

The plugin gets it data from Tromador's mirror, "http://elite.ripz.org/", but if for any reason that site goes down it will automatically fallback to downloading directly from EDDB.io's API.

Here are all the options available and a brief explanation of what each does:

    'item':         "Regenerate Categories and Items using latest commodities.json dump.",
    'system':       "Regenerate Systems using latest system-populated.jsonl dump.",
    'station':      "Regenerate Stations using latest stations.jsonl dump. (implies '-O system')",
    'ship':         "Regenerate Ships using latest coriolis.io json dump.",
    'shipvend':     "Regenerate ShipVendors using latest stations.jsonl dump. (implies '-O system,station,ship')",
    'upgrade':      "Regenerate Upgrades using latest modules.json dump.",
    'upvend':       "Regenerate UpgradeVendors using latest stations.jsonl dump. (implies '-O system,station,upgrade')",
    'listings':     "Update market data using latest listings.csv dump. (implies '-O item,system,station')",
    'all':          "Update everything with latest dumpfiles. (Regenerates all tables)",
    'clean':        "Erase entire database and rebuild from empty. (Regenerates all tables.)",
    'skipvend':     "Don't regenerate ShipVendors or UpgradeVendors. Supercedes '-O all', '-O clean'.",
    'force':        "Force regeneration of selected items even if source file not updated since previous run. "
                        "(Useful for updating Vendor tables if they were skipped during a '-O clean' run.)",
    'fallback':     "Fallback to using EDDB.io if Tromador's mirror isn't working."
